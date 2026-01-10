.libPaths(c("/home/devlin/R/aarch64-unknown-linux-gnu-library/4.2",
            "/usr/local/lib/R/site-library",
            "/usr/lib/R/site-library",
            "/usr/lib/R/library"))

library(tidyverse)
library(httr)
library(jsonlite)
library(RPostgres)
library(DBI)

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

  supersectors <- c('00', 10, 20, 31, 32, 41, 42, 43, 50, 55, 60, 65, 70, 80, 90)
  ce_series <- str_c('CEU', supersectors, '00000001')
  sm_series <- str_c('SMU1200000', supersectors, '00000001')
  series_ids <- c(ce_series, sm_series)

  jolts_all <- tibble()

  for (srs in series_ids) {
  
   srs <- ifelse(srs == 'CEU4100000001', 'CEU4142000001', srs)
  
    print(srs)
  
    payload <- str_c(
      "{",
      '"seriesid":["', srs, '"],',
      '"startyear":"2015",',
      '"endyear":"2025",',
      '"registrationkey":"', api_key,
      '"}'
    )
  
    response <- POST(url, 
                     body = payload,
                     content_type("application/json"),
                     encode = "json")
  
   x <- content(response, "text") %>% fromJSON()
  
   raw_pull <- x$Results$series$data[[1]] %>% as_tibble() %>% 
     select(-footnotes) %>%
     mutate(bls_series = srs)
  
    jolts_all <- bind_rows(jolts_all, raw_pull)
  
    Sys.sleep(5)

  }

  jolts_all <- jolts_all %>%
    mutate(prefix = str_sub(bls_series, 1, 2),
           seasonal = str_sub(bls_series, 3, 3),
           supersector = ifelse(prefix == 'SM', 
                                str_sub(bls_series, 11, 12),
                                str_sub(bls_series, 4, 5)),
           industry = ifelse(prefix == 'SM', 
                             str_sub(bls_series, 11, 18),
                             str_sub(bls_series, 4, 11)),
           value = as.numeric(value)) %>%
    rename(yr = year,
           time_period = period,
           mn = periodName,
           val = value)

  dbWriteTable(con, SQL(Sys.getenv("jolts_table")), jolts_all, overwrite = TRUE)

  dbExecute(home, "INSERT INTO monitor.job_history (job_name, status, error_message) VALUES ($1, 1, 'Success')",
            params = list("JOLTS_EMPLOYMENT"))

  dbDisconnect(con)
  dbDisconnect(home)

}, error = function(e) {

  dbExecute(home, "INSERT INTO monitor.job_history (job_name, status, error_message) VALUES ($1, 0, $2)",
            params = list("JOLTS_EMPLOYMENT", as.character(e)))

  dbDisconnect(home)
  dbDisconnect(con)
  stop(e)

})



