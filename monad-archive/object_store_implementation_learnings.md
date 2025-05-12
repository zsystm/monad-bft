# Object Store Implementation Learnings

After implementing two different approaches for multi-provider object storage support, here are the key learnings:

## Approach 1: object_store crate

### What Worked Well
- **Clean abstraction**: The object_store crate provides a unified interface that works seamlessly across AWS S3, Azure Blob Storage, and Google Cloud Storage
- **Minimal integration effort**: The trait-based design made it easy to wrap in our existing KVStore/KVReader traits
- **Well-maintained**: Being part of the Apache Arrow project ensures ongoing maintenance and updates
- **Feature-complete**: Supports streaming, async operations, and all major cloud providers

### Challenges Encountered
- **Additional dependency**: Adding object_store increased the dependency tree and binary size
- **Metrics migration**: Had to adapt from the existing AWS-specific metrics to a more generic approach
- **Loss of provider-specific features**: Some S3-specific features like RequestPayer had to be removed
- **Breaking change**: Required new CLI argument format, not backward compatible with existing configurations

### Implementation Details
- Created a new `ObjectStoreBucket` wrapper that implements our KVStore/KVReader traits
- Added provider selection logic in CLI parsing to instantiate the correct backend
- Integrated with existing enum_dispatch pattern successfully

## Approach 2: S3-Compatible API

### What Worked Well
- **Minimal code changes**: Only required adding an endpoint_url field and passing it through
- **Backward compatible**: Existing AWS S3 configurations continue to work without modification
- **Leverages existing code**: Reuses all the existing S3 implementation, metrics, and error handling
- **Wide provider support**: Works with any S3-compatible provider (Cloudflare R2, MinIO, DigitalOcean Spaces, etc.)

### Challenges Encountered
- **Provider limitations**: Relies on providers being truly S3-compatible; some edge cases may not work
- **AWS SDK overhead**: Still uses the full AWS SDK even for non-AWS providers
- **Authentication constraints**: Providers must support AWS IAM-style authentication
- **Limited to S3 API**: Can't access provider-specific optimizations or features

### Implementation Details
- Extended AwsCliArgs with optional endpoint_url field
- Modified get_aws_config to accept and configure custom endpoints
- Updated all callers to pass the endpoint URL parameter

## Comparison and Recommendations

### Use object_store approach when:
- You need native support for multiple cloud providers
- You want a clean, provider-agnostic API
- Binary size is not a critical concern
- You're starting a new project or willing to break compatibility

### Use S3-compatible approach when:
- You need to maintain backward compatibility
- Most providers you're targeting offer S3-compatible APIs
- You want minimal code changes
- You're already invested in AWS SDK features

## Key Insights

1. **Trade-offs between abstraction and simplicity**: The object_store approach provides better abstraction but requires more changes, while the S3-compatible approach is simpler but less flexible.

2. **Backward compatibility matters**: The S3-compatible approach's ability to maintain existing configurations is valuable for production systems.

3. **Provider ecosystem**: Most object storage providers offer S3-compatible APIs, making the S3-compatible approach surprisingly versatile.

4. **Maintenance burden**: The object_store approach reduces long-term maintenance by delegating provider-specific logic to a well-maintained external library.

5. **Performance considerations**: Both approaches have similar performance characteristics, as they both use async/await and streaming operations.

## Recommendation

For the monad-archive project, I recommend the **S3-compatible approach** for the following reasons:

1. **Minimal disruption**: It requires the least changes to existing code and configurations
2. **Immediate compatibility**: Works with existing deployments without migration
3. **Sufficient flexibility**: Covers the most common use cases (AWS S3, Cloudflare R2, MinIO)
4. **Pragmatic solution**: Balances functionality with simplicity

The object_store approach would be better for a greenfield project or if native support for Azure and GCP becomes a priority.