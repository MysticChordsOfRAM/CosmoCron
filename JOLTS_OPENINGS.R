.libPaths(c("/home/devlin/R/aarch64-unknown-linux-gnu-library/4.2",
            "/usr/local/lib/R/site-library",
            "/usr/lib/R/site-library",
            "/usr/lib/R/library"))

library(tidyverse)
library(httr)
library(jsonlite)
library(RPostgres)
library(DBI)

con <- NULL
home <- NULL

tryCatch({

  con <- dbConnect(drv = Postgres(),
                   dbname = Sys.getenv("db_laborgap"),
                   host = Sys.getenv("db_ip"),
                   port = as.numeric(Sys.getenv("db_port")),
                   user = Sys.getenv("db_user"),
                   password = Sys.getenv("db_password"))

  home <- dbConnect(drv = Postgres(),
                   dbname = Sys.getenv("db_home"),
                   host = Sys.getenv("db_ip"),
                   port = as.numeric(Sys.getenv("db_port")),
                   user = Sys.getenv("db_user"),
                   password = Sys.getenv("db_password"))

  api_key <- Sys.getenv("jolts_api_key")
  url <- 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

  series_detail <- dbGetQuery(con, 'SELECT * FROM jolts.series_ids')
  series_ids <- series_detail %>% pull(bls_series)

  fetch_bls <- function(srs) {

    print(str_c("Fetching ", srs))

    payload <- str_c(
      "{",
      '"seriesid":["', srs, '"],',
      '"startyear":"2015",',
      '"endyear":"2025",',
      '"registrationkey":"', api_key,
      '"}'
    )

     Sys.sleep(2)

    response <- POST(url, 
                     body = payload,
                     content_type("application/json"),
                     encode = "json")
  
    x <- content(response, "text") %>% fromJSON()

    if (length(x$Results$series$data) > 0) {
      
      raw_pull <- x$Results$series$data[[1]] %>% 
        as_tibble() %>% 
        select(-footnotes) %>%
        mutate(bls_series = srs)

      return(raw_pull)

    } else {

      print(str_c("ERROR IN ", srs))
      return(tibble())

    }
  }

  jolts_all <- map_dfr(series_ids, fetch_bls) %>%
    left_join(series_detail, by = "bls_series") %>%
    rename(yr = year, 
           time_period = period,
           mn = periodName,
           val = value)

  dbWriteTable(con, SQL(Sys.getenv("jolts_openings")), jolts_all, overwrite = TRUE)

  dbExecute(home, "INSERT INTO monitor.job_history (job_name, status, error_message) VALUES ($1, 1, 'Success')",
            params = list("JOLTS_OPENINGS"))

  dbDisconnect(con)
  dbDisconnect(home)

}, error = function(e) {

  dbExecute(home, "INSERT INTO monitor.job_history (job_name, status, error_message) VALUES ($1, 0, $2)",
            params = list("JOLTS_OPENINGS", as.character(e)))

  dbDisconnect(home)
  dbDisconnect(con)
  stop(e)

})
