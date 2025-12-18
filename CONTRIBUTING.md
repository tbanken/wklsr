# Contributing to WKLS

We welcome contributions! Whether you're fixing bugs, adding features, improving documentation, or enhancing test coverage, your help is appreciated.

## Getting Started

1. **Fork and clone** the repository
2. **Set up the development environment**:
   ```bash
   cd wkls
   ```
   ```r
   renv::restore()
   ```
3. **Run the tests** to make sure everything works:
   ```r
   devtools::test()
   ```
4. **Make your changes** and add tests if needed

5. **Submit a pull request**

## Areas for Contribution

- **Bug fixes**: Fix issues with geometry retrieval, chaining logic, or error handling
- **Performance improvements**: Optimize data loading or query performance
- **Documentation**: Improve examples, add use cases, or enhance API documentation  
- **Testing**: Add test cases for edge cases or new functionality
- **Feature requests**: Add new geometry formats or helper methods

## Before Contributing

- **Check existing issues** to see if your idea is already being discussed
- **Open an issue** for major changes to discuss the approach first
- **Add tests** for any new functionality
- **Update documentation** if you change the API

## Development Notes

- The library uses **duckdb** for spatial operations and **base** for data handling
- Geometry data comes from **Overture Maps Foundation** via S3
- Tests require internet access to fetch data from AWS Open Data Registry

## Development Commands

```r
# Install dependencies with development tools
renv::restore()

# Run tests
devtools::test()

# Run specific test file  
devtools::test_file("tests/testthat/test-us.R")
```



## Testing

We use testthat for testing. Please add tests for any new functionality:

```r
# Run tests
devtools::test()

# Run specific test
devtools::test_file("tests/testthat/test-us.R")
```

## Submitting Changes

1. **Create a feature branch**: `git checkout -b feature-name`
2. **Make your changes** with appropriate tests
3. **Ensure all tests pass**: `devtools::test()`
4. **Commit your changes**: `git commit -m "Add feature description"`
5. **Push to your fork**: `git push origin feature-name`
6. **Create a pull request** on GitHub

## Questions?

Feel free to open an issue if you have questions about contributing or need help getting started!
