package containerdstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/archive"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/remotes"
	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/pkg/streamformatter"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"

	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/backend"
	"github.com/docker/docker/api/types/filters"
	imagetypes "github.com/docker/docker/api/types/image"
	registrytypes "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/builder"
	"github.com/docker/docker/container"
	_ "github.com/docker/docker/daemon/graphdriver/register" // register graph drivers
	"github.com/docker/docker/daemon/images"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

var shortID = regexp.MustCompile(`^([a-f0-9]{4,64})$`)

type containerdStore struct {
	client *containerd.Client
}

func New(c *containerd.Client) *containerdStore {
	return &containerdStore{
		client: c,
	}
}

func (cs *containerdStore) PullImage(ctx context.Context, image, tag string, platform *ocispec.Platform, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
	opts := []containerd.RemoteOpt{
		containerd.WithPullUnpack,
	}

	if platform != nil {
		opts = append(opts, containerd.WithPlatform(platforms.Format(*platform)))
	}
	ref, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return errdefs.InvalidParameter(err)
	}

	if tag != "" {
		// The "tag" could actually be a digest.
		var dgst digest.Digest
		dgst, err = digest.Parse(tag)
		if err == nil {
			ref, err = reference.WithDigest(reference.TrimNamed(ref), dgst)
		} else {
			ref, err = reference.WithTag(ref, tag)
		}
		if err != nil {
			return errdefs.InvalidParameter(err)
		}
	}

	jobs := NewJobs()
	h := containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.MediaType != containerdimages.MediaTypeDockerSchema1Manifest {
			jobs.Add(desc)
		}
		return nil, nil
	})

	stop := make(chan struct{})
	go func() {
		ShowProgress(ctx, jobs, cs.client.ContentStore(), outStream, stop)
		stop <- struct{}{}
	}()

	img, err := cs.client.Pull(ctx, ref.String(), containerd.WithImageHandler(h))
	if err != nil {
		return err
	}

	unpacked, err := img.IsUnpacked(ctx, "overlayfs")
	if err != nil {
		return err
	}

	if !unpacked {
		if err := img.Unpack(ctx, "overlayfs"); err != nil {
			return err
		}
	}
	stop <- struct{}{}
	<-stop
	return err
}

func ShowProgress(ctx context.Context, ongoing *Jobs, cs content.Store, w io.Writer, stop chan struct{}) {
	var (
		out    = streamformatter.NewJSONProgressOutput(w, false)
		ticker = time.NewTicker(100 * time.Millisecond)
		start  = time.Now()
		done   bool
	)
	defer ticker.Stop()

outer:
	for {
		select {
		case <-ticker.C:
			if !ongoing.IsResolved() {
				continue
			}

			pulling := map[string]content.Status{}
			if !done {
				actives, err := cs.ListStatuses(ctx, "")
				if err != nil {
					log.G(ctx).WithError(err).Error("status check failed")
					continue
				}
				// update status of status entries!
				for _, status := range actives {
					pulling[status.Ref] = status
				}
			}

			// update inactive jobs
			for _, j := range ongoing.Jobs() {
				key := remotes.MakeRefKey(ctx, j)
				if info, ok := pulling[key]; ok {
					out.WriteProgress(progress.Progress{
						ID:      j.Digest.Hex(),
						Action:  "Downloading",
						Current: info.Offset,
						Total:   info.Total,
					})
					continue
				}

				info, err := cs.Info(ctx, j.Digest)
				if err != nil {
					if !cerrdefs.IsNotFound(err) {
						log.G(ctx).WithError(err).Error("failed to get content info")
						continue outer
					}
				} else if info.CreatedAt.After(start) {
					out.WriteProgress(progress.Progress{
						ID:         j.Digest.Hex(),
						Action:     "Download complete",
						HideCounts: true,
						LastUpdate: true,
					})
					ongoing.Remove(j)
				} else {
					out.WriteProgress(progress.Progress{
						ID:         j.Digest.Hex(),
						Action:     "Exists",
						HideCounts: true,
						LastUpdate: true,
					})
					ongoing.Remove(j)
				}
			}
			if done {
				return
			}
		case <-stop:
			done = true // allow ui to update once more
		}
	}
}

type StatusInfo struct {
	Ref       string
	Status    string
	Offset    int64
	Total     int64
	StartedAt time.Time
	UpdatedAt time.Time
}

type Jobs struct {
	name     string
	resolved bool
	descs    map[digest.Digest]ocispec.Descriptor
	mu       sync.Mutex
}

// NewJobs creates a new instance of the job status tracker
func NewJobs() *Jobs {
	return &Jobs{
		descs: map[digest.Digest]ocispec.Descriptor{},
	}
}

// IsResolved checks whether a descriptor has been resolved
func (j *Jobs) IsResolved() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.resolved
}

// Add adds a descriptor to be tracked
func (j *Jobs) Add(desc ocispec.Descriptor) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, ok := j.descs[desc.Digest]; ok {
		return
	}
	j.descs[desc.Digest] = desc
	j.resolved = true
}

// Remove removes a descriptor
func (j *Jobs) Remove(desc ocispec.Descriptor) {
	j.mu.Lock()
	defer j.mu.Unlock()

	delete(j.descs, desc.Digest)
}

// Jobs returns a list of all tracked descriptors
func (j *Jobs) Jobs() []ocispec.Descriptor {
	j.mu.Lock()
	defer j.mu.Unlock()

	descs := make([]ocispec.Descriptor, 0, len(j.descs))
	for _, d := range j.descs {
		descs = append(descs, d)
	}
	return descs
}

func (cs *containerdStore) Images(ctx context.Context, opts types.ImageListOptions) ([]*types.ImageSummary, error) {
	images, err := cs.client.ListImages(ctx)
	if err != nil {
		return nil, err
	}

	var ret []*types.ImageSummary
	for _, image := range images {
		size, err := image.Size(ctx)
		if err != nil {
			return nil, err
		}

		ret = append(ret, &types.ImageSummary{
			RepoDigests: []string{image.Name() + "@" + image.Target().Digest.String()}, // "hello-world@sha256:bfea6278a0a267fad2634554f4f0c6f31981eea41c553fdf5a83e95a41d40c38"},
			RepoTags:    []string{image.Name()},
			Containers:  -1,
			ParentID:    "",
			SharedSize:  -1,
			VirtualSize: 10,
			ID:          image.Target().Digest.String(),
			Created:     image.Metadata().CreatedAt.Unix(),
			Size:        size,
		})
	}

	return ret, nil
}

func (cs *containerdStore) GetLayerByID(string) (layer.RWLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetLayerMountID(string) (string, error) {
	return "", errors.New("nope")
}

func (cs *containerdStore) Cleanup() error {
	return nil
}

func (cs *containerdStore) GraphDriverName() string {
	return ""
}

func (cs *containerdStore) CommitBuildStep(context.Context, backend.CommitConfig) (image.ID, error) {
	panic("not implemented")
}

func (cs *containerdStore) CreateImage(ctx context.Context, config []byte, parent string) (builder.Image, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetImageAndReleasableLayer(ctx context.Context, refOrID string, opts backend.GetImageAndLayerOptions) (builder.Image, builder.ROLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) MakeImageCache(ctx context.Context, sourceRefs []string) (builder.ImageCache, error) {
	panic("not implemented")
}

func (cs *containerdStore) TagImageWithReference(ctx context.Context, imageID image.ID, newTag reference.Named) error {
	logrus.Infof("Tagging image %q with reference %q", imageID, newTag.String())

	desc, err := cs.ResolveImage(ctx, imageID.String())
	if err != nil {
		return err
	}

	img := containerdimages.Image{
		Name:   newTag.String(),
		Target: desc,
	}

	is := cs.client.ImageService()
	_, err = is.Create(ctx, img)

	return err
}

func (cs *containerdStore) SquashImage(ctx context.Context, id, parent string) (string, error) {
	panic("not implemented")
}

func (cs *containerdStore) ExportImage(ctx context.Context, names []string, outStream io.Writer) error {
	opts := []archive.ExportOpt{
		archive.WithPlatform(platforms.Ordered(platforms.DefaultSpec())),
	}
	is := cs.client.ImageService()
	for _, imageRef := range names {
		named, err := reference.ParseDockerRef(imageRef)
		if err != nil {
			return err
		}
		opts = append(opts, archive.WithImage(is, named.String()))
	}
	return cs.client.Export(ctx, outStream, opts...)
}

func (cs *containerdStore) ImageDelete(ctx context.Context, imageRef string, force, prune bool) ([]types.ImageDeleteResponseItem, error) {
	records := []types.ImageDeleteResponseItem{}

	parsedRef, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		return nil, err
	}
	ref := reference.TagNameOnly(parsedRef)

	if err := cs.client.ImageService().Delete(ctx, ref.String()); err != nil {
		return []types.ImageDeleteResponseItem{}, err
	}

	d := types.ImageDeleteResponseItem{Untagged: reference.FamiliarString(parsedRef)}
	records = append(records, d)

	return records, nil
}

func (cs *containerdStore) ImageHistory(ctx context.Context, name string) ([]*imagetypes.HistoryResponseItem, error) {
	panic("not implemented")
}

func (cs *containerdStore) ImagesPrune(ctx context.Context, pruneFilters filters.Args) (*types.ImagesPruneReport, error) {
	panic("not implemented")
}

func (cs *containerdStore) ImportImage(ctx context.Context, src string, repository string, platform *ocispec.Platform, tag string, msg string, inConfig io.ReadCloser, outStream io.Writer, changes []string) error {
	panic("not implemented")
}

func (cs *containerdStore) LoadImage(ctx context.Context, inTar io.ReadCloser, outStream io.Writer, quiet bool) error {
	_, err := cs.client.Import(ctx, inTar,
		containerd.WithImportPlatform(platforms.OnlyStrict(platforms.DefaultSpec())),
	)
	return err
}

func (cs *containerdStore) LookupImage(ctx context.Context, name string) (*types.ImageInspect, error) {
	panic("not implemented")
}

func (cs *containerdStore) PushImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
	panic("not implemented")
}

func (cs *containerdStore) SearchRegistryForImages(ctx context.Context, searchFilters filters.Args, term string, limit int, authConfig *types.AuthConfig, metaHeaders map[string][]string) (*registrytypes.SearchResults, error) {
	panic("not implemented")
}

func (cs *containerdStore) TagImage(ctx context.Context, imageName, repository, tag string) (string, error) {
	desc, err := cs.ResolveImage(ctx, imageName)
	if err != nil {
		return "", err
	}

	newTag, err := reference.ParseNormalizedNamed(repository)
	if err != nil {
		return "", err
	}
	if tag != "" {
		if newTag, err = reference.WithTag(reference.TrimNamed(newTag), tag); err != nil {
			return "", err
		}
	}

	err = cs.TagImageWithReference(ctx, image.ID(desc.Digest), newTag)
	return reference.FamiliarString(newTag), err
}

func (cs *containerdStore) GetRepository(context.Context, reference.Named, *types.AuthConfig) (distribution.Repository, error) {
	panic("not implemented")
}

func (cs *containerdStore) ImageDiskUsage(ctx context.Context) ([]*types.ImageSummary, error) {
	panic("not implemented")
}

func (cs *containerdStore) LayerDiskUsage(ctx context.Context) (int64, error) {
	panic("not implemented")
}

func (cs *containerdStore) ReleaseLayer(rwlayer layer.RWLayer) error {
	panic("not implemented")
}

func (cs *containerdStore) CommitImage(ctx context.Context, c backend.CommitConfig) (image.ID, error) {
	panic("not implemented")
}

func (cs *containerdStore) GetImage(ctx context.Context, refOrID string, platform *ocispec.Platform) (im containerdimages.Image, retImg *image.Image, retErr error) {
	desc, err := cs.ResolveImage(ctx, refOrID)
	if err != nil {
		return im, nil, err
	}

	im, err = cs.resolveImageName2(ctx, refOrID)
	if err != nil {
		return im, nil, err
	}
	ii := containerd.NewImage(cs.client, im)
	provider := cs.client.ContentStore()
	conf, err := im.Config(ctx, provider, ii.Platform())
	if err != nil {
		return im, nil, err
	}

	var ociimage v1.Image
	imageConfigBytes, err := content.ReadBlob(ctx, ii.ContentStore(), conf)
	if err != nil {
		return im, nil, err
	}

	if err := json.Unmarshal(imageConfigBytes, &ociimage); err != nil {
		return im, nil, err
	}

	return im, &image.Image{
		V1Image: image.V1Image{
			ID:           string(desc.Digest),
			OS:           ociimage.OS,
			Architecture: ociimage.Architecture,
			Config: &containertypes.Config{
				Entrypoint: ociimage.Config.Entrypoint,
				Env:        ociimage.Config.Env,
				Cmd:        ociimage.Config.Cmd,
				User:       ociimage.Config.User,
				WorkingDir: ociimage.Config.WorkingDir,
			},
		},
	}, nil
}

func (cs *containerdStore) GetContainerdImage(ctx context.Context, refOrID string, platform *ocispec.Platform) (retImg containerd.Image, retErr error) {
	im, err := cs.resolveImageName2(ctx, refOrID)
	if err != nil {
		return nil, err
	}
	ii := containerd.NewImage(cs.client, im)
	return ii, nil
}

func (cs *containerdStore) CreateLayer(ctx context.Context, container *container.Container, initFunc layer.MountInit) (layer.RWLayer, error) {
	panic("not implemented")
}

func (cs *containerdStore) DistributionServices() images.DistributionServices {
	return images.DistributionServices{}
}

func (cs *containerdStore) CountImages(ctx context.Context) (int, error) {
	imgs, err := cs.client.ListImages(ctx)
	if err != nil {
		return 0, err
	}

	return len(imgs), nil
}

func (cs *containerdStore) LayerStoreStatus() [][2]string {
	panic("not implemented")
}

func (cs *containerdStore) GetContainerLayerSize(containerID string) (int64, int64) {
	panic("not implemented")
}

func (cs *containerdStore) UpdateConfig(maxDownloads, maxUploads *int) {
	panic("not implemented")
}

func (cs *containerdStore) Children(ctx context.Context, id image.ID) ([]image.ID, error) {
	panic("not implemented")
}

// ResolveImage searches for an image based on the given
// reference or identifier. Returns the descriptor of
// the image, could be manifest list, manifest, or config.
func (cs *containerdStore) ResolveImage(ctx context.Context, refOrID string) (d ocispec.Descriptor, err error) {
	d, _, err = cs.resolveImageName(ctx, refOrID)
	return
}

func (cs *containerdStore) resolveImageName2(ctx context.Context, refOrID string) (containerdimages.Image, error) {
	parsed, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		return containerdimages.Image{}, errdefs.InvalidParameter(err)
	}

	is := cs.client.ImageService()

	namedRef, ok := parsed.(reference.Named)
	if !ok {
		digested, ok := parsed.(reference.Digested)
		if !ok {
			return containerdimages.Image{}, errdefs.InvalidParameter(errors.New("bad reference"))
		}

		imgs, err := is.List(ctx, fmt.Sprintf("target.digest==%s", digested.Digest()))
		if err != nil {
			return containerdimages.Image{}, errors.Wrap(err, "failed to lookup digest")
		}
		if len(imgs) == 0 {
			return containerdimages.Image{}, errdefs.NotFound(errors.New("image not found with digest"))
		}

		return imgs[0], nil
	}

	namedRef = reference.TagNameOnly(namedRef)

	// If the identifier could be a short ID, attempt to match
	if shortID.MatchString(refOrID) {
		ref := namedRef.String()
		filters := []string{
			fmt.Sprintf("name==%q", ref),
			fmt.Sprintf(`target.digest~=/sha256:%s[0-9a-fA-F]{%d}/`, refOrID, 64-len(refOrID)),
		}
		imgs, err := is.List(ctx, filters...)
		if err != nil {
			return containerdimages.Image{}, err
		}

		if len(imgs) == 0 {
			return containerdimages.Image{}, errdefs.NotFound(errors.New("list returned no images"))
		}
		if len(imgs) > 1 {
			digests := map[digest.Digest]struct{}{}
			for _, img := range imgs {
				if img.Name == ref {
					return img, nil
				}
				digests[img.Target.Digest] = struct{}{}
			}

			if len(digests) > 1 {
				return containerdimages.Image{}, errdefs.NotFound(errors.New("ambiguous reference"))
			}
		}

		if imgs[0].Name != ref {
			namedRef = nil
		}
		return imgs[0], nil
	}
	img, err := is.Get(ctx, namedRef.String())
	if err != nil {
		// TODO(containerd): error translation can use common function
		if !cerrdefs.IsNotFound(err) {
			return containerdimages.Image{}, err
		}
		return containerdimages.Image{}, errdefs.NotFound(errors.New("id not found"))
	}

	return img, nil
}

func (cs *containerdStore) resolveImageName(ctx context.Context, refOrID string) (ocispec.Descriptor, reference.Named, error) {
	parsed, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		return ocispec.Descriptor{}, nil, errdefs.InvalidParameter(err)
	}

	is := cs.client.ImageService()

	namedRef, ok := parsed.(reference.Named)
	if !ok {
		digested, ok := parsed.(reference.Digested)
		if !ok {
			return ocispec.Descriptor{}, nil, errdefs.InvalidParameter(errors.New("bad reference"))
		}

		imgs, err := is.List(ctx, fmt.Sprintf("target.digest==%s", digested.Digest()))
		if err != nil {
			return ocispec.Descriptor{}, nil, errors.Wrap(err, "failed to lookup digest")
		}
		if len(imgs) == 0 {
			return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("image not found with digest"))
		}

		return imgs[0].Target, nil, nil
	}

	namedRef = reference.TagNameOnly(namedRef)

	// If the identifier could be a short ID, attempt to match
	if shortID.MatchString(refOrID) {
		ref := namedRef.String()
		filters := []string{
			fmt.Sprintf("name==%q", ref),
			fmt.Sprintf(`target.digest~=/sha256:%s[0-9a-fA-F]{%d}/`, refOrID, 64-len(refOrID)),
		}
		imgs, err := is.List(ctx, filters...)
		if err != nil {
			return ocispec.Descriptor{}, nil, err
		}

		if len(imgs) == 0 {
			return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("list returned no images"))
		}
		if len(imgs) > 1 {
			digests := map[digest.Digest]struct{}{}
			for _, img := range imgs {
				if img.Name == ref {
					return img.Target, namedRef, nil
				}
				digests[img.Target.Digest] = struct{}{}
			}

			if len(digests) > 1 {
				return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("ambiguous reference"))
			}
		}

		if imgs[0].Name != ref {
			namedRef = nil
		}
		return imgs[0].Target, namedRef, nil
	}
	img, err := is.Get(ctx, namedRef.String())
	if err != nil {
		// TODO(containerd): error translation can use common function
		if !cerrdefs.IsNotFound(err) {
			return ocispec.Descriptor{}, nil, err
		}
		return ocispec.Descriptor{}, nil, errdefs.NotFound(errors.New("id not found"))
	}

	return img.Target, namedRef, nil
}
