# Release Checklist for v0.1.0

## Pre-Release Checklist

### Code Quality
- [x] Code formatted with `cargo fmt`
- [x] All tests passing (`cargo test`)
- [x] Clippy checks pass (`cargo clippy -- -D warnings`)
- [x] Documentation builds (`cargo doc --no-deps`)
- [x] Examples compile (`cargo build --examples`)

### Documentation
- [x] README.md updated with comprehensive documentation
- [x] API documentation added to public functions
- [x] Examples created and documented
- [x] Architecture documentation up to date
- [x] Contributing guidelines in place

### Cargo.toml
- [x] Version set to `0.1.0`
- [x] Authors, description, license set
- [x] Repository and homepage URLs correct
- [x] Keywords and categories appropriate
- [x] Readme field points to README.md
- [x] Examples configured

### Testing
- [x] Unit tests (`tests/unit_test.rs`)
- [x] Integration tests (`tests/integration_test.rs`)
- [x] Property-based tests (`tests/property_test.rs`)

## Release Steps

### 1. Final Verification

```bash
# Format code
cargo fmt

# Run all tests
cargo test

# Check for warnings
cargo clippy -- -D warnings

# Build documentation
cargo doc --no-deps --open

# Verify examples compile
cargo build --examples
```

### 2. Update Version

Ensure `Cargo.toml` has:
```toml
version = "0.1.0"
```

### 3. Create Git Tag

```bash
git add .
git commit -m "Release v0.1.0"
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin main
git push origin v0.1.0
```

### 4. Publish to Crates.io

```bash
# Dry run first
cargo publish --dry-run

# If successful, publish
cargo publish
```

**Note:** You'll need:
- A crates.io account
- An API token from https://crates.io/me
- Run `cargo login <token>` first

### 5. Post-Release

- [ ] Update CHANGELOG.md (if you have one)
- [ ] Announce release on social media/forums
- [ ] Update any external documentation
- [ ] Monitor for issues

## Publishing Checklist

Before running `cargo publish`, ensure:

1. **Cargo.toml is correct:**
   - [x] Package name is available on crates.io
   - [x] All metadata fields are filled
   - [x] Dependencies are correct
   - [x] No path dependencies (unless publishing workspace)

2. **Code is ready:**
   - [x] All code is formatted
   - [x] All tests pass
   - [x] No warnings from clippy
   - [x] Documentation is complete

3. **Files are included:**
   - [x] README.md
   - [x] LICENSE
   - [x] Examples (if any)
   - [x] Source code

4. **Files are excluded:**
   - [x] target/ directory
   - [x] .git/ directory
   - [x] Temporary files
   - [x] State directories

## Common Issues

### "crate already exists"
- Check if the name is already taken
- Consider using a different name or contacting the owner

### "API token invalid"
- Regenerate token at https://crates.io/me
- Run `cargo login` again

### "Documentation build failed"
- Check for broken links in doc comments
- Ensure all public APIs are documented
- Run `cargo doc --no-deps` locally first

## Version Bumping

For future releases:

- **Patch (0.1.1)**: Bug fixes, no API changes
- **Minor (0.2.0)**: New features, backward compatible
- **Major (1.0.0)**: Breaking changes

Follow [Semantic Versioning](https://semver.org/).

