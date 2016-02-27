package storage

import (
	"path"

	"github.com/tonyhb/distribution/context"
	"github.com/tonyhb/distribution/digest"
)

type signatureStore struct {
	repository *repository
	blobStore  *blobStore
	ctx        context.Context
}

func (s *signatureStore) Get(dgst digest.Digest) ([][]byte, error) {
	signaturesPath, err := pathFor(manifestSignaturesPathSpec{
		name:     s.repository.Named().Name(),
		revision: dgst,
	})

	if err != nil {
		return nil, err
	}

	// Need to append signature digest algorithm to path to get all items.
	// Perhaps, this should be in the pathMapper but it feels awkward. This
	// can be eliminated by implementing listAll on drivers.
	signaturesPath = path.Join(signaturesPath, "sha256")

	signaturePaths, err := s.blobStore.driver.List(s.ctx, signaturesPath)
	if err != nil {
		return nil, err
	}

	if len(signaturePaths) < 1 {
		context.GetLogger(s.ctx).Errorf("no signatures found at path: %q", signaturesPath)
		return [][]byte{}, nil
	}

	sigPath := signaturePaths[0]

	bs := s.linkedBlobStore(s.ctx, dgst)

	sigdgst, err := digest.ParseDigest("sha256:" + path.Base(sigPath))
	if err != nil {
		context.GetLogger(s.ctx).Errorf("could not get digest from path: %q, skipping", sigPath)
		return [][]byte{}, nil
	}

	context.GetLogger(s.ctx).Debugf("fetching signature %q", sigdgst)

	signature, err := bs.Get(s.ctx, sigdgst)
	if err != nil {
		context.GetLogger(s.ctx).Errorf("error fetching signature %q: %v", sigdgst, err)
		return [][]byte{}, nil
	}

	return [][]byte{signature}, err
}

func (s *signatureStore) Put(dgst digest.Digest, signatures ...[]byte) error {
	bs := s.linkedBlobStore(s.ctx, dgst)
	for _, signature := range signatures {
		if _, err := bs.Put(s.ctx, "application/json", signature); err != nil {
			return err
		}
	}
	return nil
}

// linkedBlobStore returns the namedBlobStore of the signatures for the
// manifest with the given digest. Effectively, each signature link path
// layout is a unique linked blob store.
func (s *signatureStore) linkedBlobStore(ctx context.Context, revision digest.Digest) *linkedBlobStore {
	linkpath := func(name string, dgst digest.Digest) (string, error) {
		return pathFor(manifestSignatureLinkPathSpec{
			name:      name,
			revision:  revision,
			signature: dgst,
		})

	}

	return &linkedBlobStore{
		ctx:        ctx,
		repository: s.repository,
		blobStore:  s.blobStore,
		blobAccessController: &linkedBlobStatter{
			blobStore:   s.blobStore,
			repository:  s.repository,
			linkPathFns: []linkPathFunc{linkpath},
		},
		linkPathFns: []linkPathFunc{linkpath},
	}
}
