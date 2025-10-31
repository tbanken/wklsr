# wkls.R - R implementation of the wkls library

library(duckdb)

# Package environment to store connection
.wkls_env <- new.env()

# Overture Maps dataset version
OVERTURE_VERSION <- "2025-09-24.0"
S3_PARQUET_PATH <- sprintf("s3://overturemaps-us-west-2/release/%s/theme=divisions/type=division_area/*",
                           OVERTURE_VERSION)

# SQL Queries
COUNTRY_QUERY <- "
    SELECT * FROM wkls
    WHERE country = ?
      AND subtype = 'country'
"

REGION_QUERY <- "
    SELECT * FROM wkls
    WHERE country = ?
      AND region = ?
      AND subtype = 'region'
"

CITY_QUERY <- "
    SELECT * FROM wkls
    WHERE country = ?
      AND region = ?
      AND subtype IN ('county', 'locality', 'localadmin')
      AND REPLACE(name, ' ', '') ILIKE REPLACE(?, ' ', '')
"

#' Initialize the wkls table
#' @keywords internal
.initialize_table <- function() {
  if (exists("con", envir = .wkls_env)) {
    return(invisible(NULL))
  }

  # Create DuckDB connection
  .wkls_env$con <- dbConnect(duckdb::duckdb())

  # Get path to data file - try multiple locations
  # 1. Try inst/extdata (standard R package location for external data)
  data_path <- system.file("extdata", "overture.zstd18.parquet", package = "wklsr")

  # 2. If not found, try inst/data
  if (data_path == "" || !file.exists(data_path)) {
    data_path <- system.file("data", "overture.zstd18.parquet", package = "wklsr")
  }

  # 3. If running as a script (not installed package), look in local folders
  if (data_path == "" || !file.exists(data_path)) {
    # Try inst/extdata relative to source
    if (file.exists("R/inst/extdata/overture.zstd18.parquet")) {
      data_path <- "R/inst/extdata/overture.zstd18.parquet"
    } else {
      stop("Could not find overture.zstd18.parquet data file. Please ensure it is placed in inst/extdata/ directory before building the package.")
    }
  }

  # Install and load extensions, configure S3, and create table
  dbExecute(.wkls_env$con, "INSTALL spatial")
  dbExecute(.wkls_env$con, "LOAD spatial")
  dbExecute(.wkls_env$con, "INSTALL httpfs")
  dbExecute(.wkls_env$con, "LOAD httpfs")

  dbExecute(.wkls_env$con, "SET s3_region='us-west-2'")
  dbExecute(.wkls_env$con, "SET s3_access_key_id=''")
  dbExecute(.wkls_env$con, "SET s3_secret_access_key=''")
  dbExecute(.wkls_env$con, "SET s3_session_token=''")
  dbExecute(.wkls_env$con, "SET s3_endpoint='s3.amazonaws.com'")
  dbExecute(.wkls_env$con, "SET s3_use_ssl=true")

  # Create table from parquet file
  query <- sprintf("
    CREATE TABLE IF NOT EXISTS wkls AS
    SELECT id, country, region, subtype, name
    FROM '%s'
  ", data_path)

  dbExecute(.wkls_env$con, query)
}

# Initialize on load
.initialize_table()

#' Internal function to resolve a chain
#' @keywords internal
.resolve_chain <- function(chain) {
  .initialize_table()

  if (length(chain) == 0) {
    stop("No attributes in the chain. Use wkls$country or wkls$country$region, etc.")
  } else if (length(chain) == 1) {
    country_iso <- toupper(chain[1])
    query <- COUNTRY_QUERY
    params <- list(country_iso)
  } else if (length(chain) == 2) {
    country_iso <- toupper(chain[1])
    region_iso <- paste0(country_iso, "-", toupper(chain[2]))
    query <- REGION_QUERY
    params <- list(country_iso, region_iso)
  } else if (length(chain) == 3) {
    country_iso <- toupper(chain[1])
    region_iso <- paste0(country_iso, "-", toupper(chain[2]))
    query <- CITY_QUERY
    params <- list(country_iso, region_iso, chain[3])
  } else {
    stop("Too many chained attributes (max = 3)")
  }

  return(dbGetQuery(.wkls_env$con, query, params = params))
}

#' Internal function to get geometry
#' @keywords internal
.get_geom_expr <- function(chain, expr) {
  df <- .resolve_chain(chain)
  if (nrow(df) == 0) {
    stop(sprintf("No result found for: %s", paste(chain, collapse = ".")))
  }

  geom_id <- df$id[1]
  query <- sprintf("
    SELECT %s
    FROM parquet_scan('%s')
    WHERE id = '%s'
  ", expr, S3_PARQUET_PATH, geom_id)

  result_df <- dbGetQuery(.wkls_env$con, query)
  if (nrow(result_df) == 0) {
    stop(sprintf("No geometry found for ID: %s", geom_id))
  }
  return(result_df[1, 1])
}

#' Create a wkls proxy object
#' @keywords internal
.make_wkls_proxy <- function(chain = character(0)) {
  obj <- list()  # Empty list - don't store chain here!
  class(obj) <- "wkls_proxy"
  attr(obj, "wkls_chain") <- chain
  obj
}

#' Print method for wkls_proxy
#' @export
print.wkls_proxy <- function(x, ...) {
  # Access chain from attribute
  chain <- attr(x, "wkls_chain", exact = TRUE)
  df <- .resolve_chain(chain)
  print(df)
  invisible(x)
}

#' $ operator for wkls_proxy
#' @export
`$.wkls_proxy` <- function(x, name) {
  # CRITICAL: Access chain from attribute to avoid triggering $ again
  chain <- attr(x, "wkls_chain", exact = TRUE)

  # Special handling for accessing the chain field itself
  if (name == "chain") {
    return(chain)
  }

  # Method names
  method_names <- c("wkt", "wkb", "hexwkb", "geojson", "svg",
                    "countries", "regions", "counties", "cities", "subtypes",
                    "overture_version")

  if (name %in% method_names) {
    # Return a function bound to this chain
    chain <- x$chain

    if (name == "wkt") {
      return(function() .get_geom_expr(chain, "ST_AsText(geometry)"))
    } else if (name == "wkb") {
      return(function() .get_geom_expr(chain, "ST_AsWKB(geometry)"))
    } else if (name == "hexwkb") {
      return(function() .get_geom_expr(chain, "ST_AsHEXWKB(geometry)"))
    } else if (name == "geojson") {
      return(function() .get_geom_expr(chain, "ST_AsGeoJSON(geometry)"))
    } else if (name == "svg") {
      return(function(relative = FALSE, precision = 15L) {
        expr <- sprintf("ST_AsSVG(geometry, %s, %d)",
                        tolower(as.character(relative)),
                        as.integer(precision))
        .get_geom_expr(chain, expr)
      })
    } else if (name == "countries") {
      return(function() {
        if (length(chain) > 0) {
          stop("countries() can only be called on the root object.")
        }
        .initialize_table()
        dbGetQuery(.wkls_env$con, "
          SELECT DISTINCT id, country, subtype, name
          FROM wkls
          WHERE subtype = 'country'
        ")
      })
    } else if (name == "regions") {
      return(function() {
        if (length(chain) != 1) {
          stop("regions() requires exactly one level of chaining. Use wkls$country$regions()")
        }
        .initialize_table()
        country_iso <- toupper(chain[1])
        dbGetQuery(.wkls_env$con, sprintf("
          SELECT * FROM wkls
          WHERE country = '%s' AND subtype = 'region'
        ", country_iso))
      })
    } else if (name == "counties") {
      return(function() {
        if (length(chain) != 2) {
          stop("counties() requires exactly two levels of chaining. Use wkls$country$region$counties()")
        }
        .initialize_table()
        country_iso <- toupper(chain[1])
        region_iso <- paste0(country_iso, "-", toupper(chain[2]))
        dbGetQuery(.wkls_env$con, sprintf("
          SELECT * FROM wkls
          WHERE country = '%s' AND region = '%s' AND subtype = 'county'
        ", country_iso, region_iso))
      })
    } else if (name == "cities") {
      return(function() {
        if (length(chain) != 2) {
          stop("cities() requires exactly two levels of chaining. Use wkls$country$region$cities()")
        }
        .initialize_table()
        country_iso <- toupper(chain[1])
        region_iso <- paste0(country_iso, "-", toupper(chain[2]))
        dbGetQuery(.wkls_env$con, sprintf("
          SELECT * FROM wkls
          WHERE country = '%s' AND region = '%s' AND subtype IN ('locality', 'localadmin')
        ", country_iso, region_iso))
      })
    } else if (name == "subtypes") {
      return(function() {
        if (length(chain) > 0) {
          stop("subtypes() can only be called on the root object.")
        }
        .initialize_table()
        dbGetQuery(.wkls_env$con, "SELECT DISTINCT subtype FROM wkls")
      })
    } else if (name == "overture_version") {
      return(function() {
        if (length(chain) > 0) {
          stop("overture_version() is only available at the root level.")
        }
        OVERTURE_VERSION
      })
    }
  }

  # Chain another attribute
  new_chain <- c(chain, tolower(name))
  if (length(new_chain) > 3) {
    stop("Too many chained attributes (max = 3)")
  }

  .make_wkls_proxy(new_chain)
}

#' [[ operator for wkls_proxy
#' @export
`[[.wkls_proxy` <- function(x, name) {
  # Access chain from attribute
  chain <- attr(x, "wkls_chain", exact = TRUE)

  # For pattern searches with %
  name_lower <- tolower(name)
  new_chain <- c(chain, name_lower)

  if (length(new_chain) > 3 && !grepl("%", name)) {
    stop("Too many chained attributes (max = 3)")
  }

  # If contains %, resolve immediately
  if (grepl("%", name)) {
    return(.resolve_chain(new_chain))
  }

  .make_wkls_proxy(new_chain)
}

# Create the main wkls object
#' @export
wkls <- .make_wkls_proxy()

# Cleanup function
.onUnload <- function(libpath) {
  if (exists("con", envir = .wkls_env)) {
    dbDisconnect(.wkls_env$con, shutdown = TRUE)
  }
}
