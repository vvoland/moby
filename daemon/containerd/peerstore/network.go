package peerstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/log"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

type ContentNetwork struct {
	Host   host.Host
	PubSub *pubsub.PubSub
	Topic  *pubsub.Topic
	Sub    *pubsub.Subscription
	Store  content.Store

	images  map[string]map[peer.ID]ocispec.Descriptor
	content map[string]map[peer.ID]struct{}

	peerImages   map[peer.ID]map[string]struct{}
	peerContents map[peer.ID]map[string]struct{}

	mutex sync.Mutex
}

const TopicID = "moby-contentstore"

func (net *ContentNetwork) readContentHandler(stream network.Stream) {
	ctx := context.Background()
	buf := bytes.Buffer{}
	max := int64(len([]byte("sha256:cbc80bb5c0c0f8944bf73b3a429505ac5cde16644978bc9a1e74c5755f8ca556")))

	n, err := io.CopyN(&buf, stream, max)
	if err != nil || n != max {
		log.G(ctx).Warn("invalid digest read")
		return
	}

	defer stream.Close()

	dgst, err := digest.Parse(buf.String())
	if err != nil {
		log.G(ctx).WithError(err).Warn("[p2p] invalid digest")
		return
	}

	log.G(ctx).WithField("digest", dgst).Debug("[p2p] returning content")
	rd, err := net.Store.ReaderAt(ctx, ocispec.Descriptor{Digest: dgst})
	if err != nil {
		log.G(ctx).WithField("digest", dgst).Warn("[p2p] unknown digest")
		return
	}

	bufWriter := bufio.NewWriter(stream)
	defer bufWriter.Flush()

	if n, err := io.Copy(bufWriter, content.NewReader(rd)); err != nil {
		log.G(ctx).WithField("digest", dgst).WithField("n", n).Warn("[p2p] failed to read content")
		return
	}

}

func NewNetwork() (*ContentNetwork, error) {
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		return nil, err
	}

	topic, err := ps.Join(TopicID)
	if err != nil {
		return nil, err
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}

	return &ContentNetwork{
		Host:   host,
		PubSub: ps,
		Topic:  topic,
		Sub:    sub,

		images:       make(map[string]map[peer.ID]ocispec.Descriptor),
		content:      make(map[string]map[peer.ID]struct{}),
		peerImages:   make(map[peer.ID]map[string]struct{}),
		peerContents: make(map[peer.ID]map[string]struct{}),
	}, nil
}

const PeerAdvertisementMagic = 0x50145D1351505341

type PeerAdvertisement struct {
	Magic   uint64                        `json:","`
	Images  map[string]ocispec.Descriptor `json:","` // image references
	Content []string                      `json:","`
}

func (net *ContentNetwork) Run(ctx context.Context, cs content.Store) {
	net.Host.SetStreamHandler(ContentStreamID, net.readContentHandler)
	net.Store = cs

	log.G(ctx).Infof("[p2p] I AM %s", net.Host.ID())
	go net.discoverPeers(ctx)

	for {
		m, err := net.Sub.Next(ctx)
		if err != nil {
			log.G(ctx).WithError(err).Warn("[p2p] failed to get message")
			continue
		}

		if m.ReceivedFrom == net.Host.ID() {
			// Ignore self advertisements.
			continue
		}

		var advert PeerAdvertisement
		if err := json.Unmarshal(m.Data, &advert); err != nil {
			log.G(ctx).WithField("from", m.ReceivedFrom).WithError(err).Warn("[p2p] invalid message received, ignoring")
			continue
		}

		if advert.Magic != PeerAdvertisementMagic {
			log.G(ctx).WithField("from", m.ReceivedFrom).WithError(err).Warn("[p2p] invalid message received (bad magic), ignoring")
			continue
		}

		net.Update(advert, m.ReceivedFrom)
	}
}

func removePeer[K comparable, V any](col map[K]map[peer.ID]V, peer peer.ID, key K) {
	peers, ok := col[key]
	if !ok {
		// Image wasn't associated with this peer.
		return
	}

	delete(peers, peer)
}

func (net *ContentNetwork) Resolve(_ context.Context, imageRef string) (ocispec.Descriptor, error) {
	peers, known := net.images[imageRef]
	if !known {
		return ocispec.Descriptor{}, errdefs.NotFound(errors.New("image unknown to network"))
	}

	var result *ocispec.Descriptor
	for _, desc := range peers {
		if result == nil {
			result = &desc
			continue
		}

		if result.Digest.String() != desc.Digest.String() {
			return desc, errdefs.Conflict(errors.New("image resolves to a different digest amongst peers"))
		}
	}
	if result == nil {
		return ocispec.Descriptor{}, errdefs.NotFound(errors.New("image unknown to network"))
	}

	return *result, nil
}

const ContentStreamID = "/1.0.0/content"

func (net *ContentNetwork) readContentFromPeer(ctx context.Context, peer peer.ID, dgst string) (_ io.ReadCloser, outErr error) {
	stream, err := net.Host.NewStream(ctx, peer, ContentStreamID)
	if err != nil {
		log.G(ctx).WithField("peer", peer).WithField("digest", dgst).WithError(err).Warn("[p2p] failed to open stream to fetch content")
		return nil, err
	}

	defer func() {
		if outErr != nil {
			stream.Close()
		}
	}()

	err = stream.SetWriteDeadline(time.Now().Add(time.Second * 5))
	if err != nil {
		log.G(ctx).WithField("peer", peer).WithError(err).Warn("[p2p] failed to set deadline on peer stream")
		return nil, err
	}

	log.G(ctx).WithField("peer", peer).Infof("[p2p] fetch %s", dgst)
	msg := []byte(dgst)

	n, err := io.Copy(stream, bytes.NewReader(msg))
	if err != nil || n != int64(len(msg)) {
		log.G(ctx).WithField("peer", peer).WithError(err).Warn("[p2p] failed to send digest")
		return nil, err
	}

	return stream, err
}

func (net *ContentNetwork) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	dgst := desc.Digest.String()

	for peer := range net.content[dgst] {
		reader, err := net.readContentFromPeer(ctx, peer, dgst)
		if err != nil {
			continue
		}

		return reader, nil
	}

	return nil, errdefs.NotFound(errors.New("content unknown to network"))
}

func (net *ContentNetwork) Advertise(ctx context.Context, is images.Store, cs content.Store) error {
	advert := PeerAdvertisement{
		Magic:   PeerAdvertisementMagic,
		Images:  make(map[string]ocispec.Descriptor),
		Content: nil,
	}

	err := cs.Walk(ctx, func(i content.Info) error {
		advert.Content = append(advert.Content, i.Digest.String())
		return nil
	})
	if err != nil {
		return err
	}

	images, err := is.List(ctx)
	if err != nil {
		return err
	}

	for _, img := range images {
		advert.Images[img.Name] = img.Target
	}

	data, err := json.Marshal(advert)
	if err != nil {
		return err
	}

	log.G(context.TODO()).WithField("advert", advert).Debug("[p2p] advertise")

	return net.Topic.Publish(ctx, data)
}

func (net *ContentNetwork) Update(advert PeerAdvertisement, from peer.ID) {
	net.mutex.Lock()
	defer net.mutex.Unlock()

	newPeerImages := map[string]struct{}{}
	newPeerContents := map[string]struct{}{}

	for imgRef, desc := range advert.Images {
		newPeerImages[imgRef] = struct{}{}

		peers := net.images[imgRef]
		if peers == nil {
			net.images[imgRef] = make(map[peer.ID]ocispec.Descriptor)
		}

		net.images[imgRef][from] = desc
	}
	for _, dgst := range advert.Content {
		newPeerContents[dgst] = struct{}{}
		peers := net.content[dgst]

		if peers == nil {
			peers = make(map[peer.ID]struct{})
			net.content[dgst] = peers
		}
		peers[from] = struct{}{}
	}

	// Delete images, that peer previously advertisted, but wasn't advertised now.
	for image, _ := range net.peerImages[from] {
		if _, has := newPeerImages[image]; !has {
			removePeer(net.images, from, image)
		}
	}

	// Delete content, that peer previously advertisted, but wasn't advertised now.
	for dgst, _ := range net.peerContents[from] {
		if _, has := newPeerContents[dgst]; !has {
			removePeer(net.content, from, dgst)
		}
	}

	net.peerImages[from] = newPeerImages
	net.peerContents[from] = newPeerContents

	for imgRef, peers := range net.images {
		log.G(context.TODO()).Debugf("[p2p] %d peers know about image %s", len(peers), imgRef)
	}
	for dgst, peers := range net.content {
		log.G(context.TODO()).Debugf("[p2p] %d peers know about content %s", len(peers), dgst)
	}
}

func initDHT(ctx context.Context, h host.Host) *dht.IpfsDHT {
	// Start a DHT, for use in peer discovery. We can't just make a new DHT
	// client because we want each peer to maintain its own local copy of the
	// DHT, so that the bootstrapping node of the DHT can go down without
	// inhibiting future peer discovery.
	kademliaDHT, err := dht.New(ctx, h)
	if err != nil {
		panic(err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for _, peerAddr := range dht.DefaultBootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Connect(ctx, *peerinfo); err != nil {
				log.G(ctx).WithError(err).Warn("[p2p] bootstrap warning")
			}
		}()
	}
	wg.Wait()

	return kademliaDHT
}

func (net *ContentNetwork) discoverPeers(ctx context.Context) {
	h := net.Host
	kademliaDHT := initDHT(ctx, h)
	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, TopicID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log.G(ctx).Debug("[p2p] search for peers")
		peerChan, err := routingDiscovery.FindPeers(ctx, TopicID)
		if err != nil {
			panic(err)
		}
		for peer := range peerChan {
			if peer.ID == h.ID() {
				continue // No self connection
			}

			connectedness := h.Network().Connectedness(peer.ID)
			if connectedness == network.Connected {
				continue
			}

			err := h.Connect(ctx, peer)
			if err != nil {
				if errors.Is(err, swarm.ErrNoAddresses) {
					continue
				}
				if errors.Is(err, swarm.ErrDialBackoff) {
					continue
				}
				//log.G(ctx).WithField("peer", peer).WithError(err).Warn("[p2p] failed peer connection")
			} else {
				log.G(ctx).WithField("peer", peer).Infof("[p2p] connected to peer")
			}
		}

		time.Sleep(time.Second)
	}
}
