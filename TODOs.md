## HighPri
    [ ] add private registry interface.
    [ ] add proxy registry interface and framework implementation.
    [ ] add compound (proxy+private) interface and framework implementation.
    [ ] implement compound interface for docker registry.
    [ ] implement registry manager (CRUD for registries, storage choosing...)
## MidPri
    [ ] implement s3 implementation of ArtifactStorage
## LowPri
    [ ] multipart create/upload interface and implementation (check S3 or similar)
    [ ] distributed (sharded & replicated) storage (check S3 or similar)
    [ ] implements S3 or similar service on mulipart and distibuted storage completion.
    [ ] Consider limiting minimum hashlength of 3 characters, in low-level ArtifactStorage implementations.
    [ ] DockerRegistryProxyClient is a generic client; can be refactor as one.
     
