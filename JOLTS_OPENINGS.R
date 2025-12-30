.libPaths(c("/home/devlin/R/aarch64-unknown-linux-gnu-library/4.2",
            "/usr/local/lib/R/site-library",
            "/usr/lib/R/site-library",
            "/usr/lib/R/library"))

library(tidyverse)
library(httr)
library(jsonlite)
library(RPostgres)
library(DBI)

con <- dbConnect(drv = Postgres(),
                 dbname = Sys.getenv("db_laborgap"),
                 host = Sys.getenv("db_ip"),
                 port = as.numeric(Sys.getenv("db_port"),
                 user = Sys.getenv("db_user"),
                 password = Sys.getenv("db_password"))

api_key <- Sys.getenv("jolts_api_key")

url <- 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

series_detail <- dbGetQuery(con, 'SELECT * FROM jolts.series_ids')

series_ids <- series_detail %>% pull(bls_series)

jolts_all <- tibble()

for (srs in series_ids) {
  
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
    mutate(bls_series = srs) %>%
    left_join(series_detail)
  
  jolts_all <- bind_rows(jolts_all, raw_pull)
  
}

final <- jolts_all %>%
  rename(yr = year, 
         time_period = period,
         mn = periodName,
         val = value)

dbWriteTable(con, SQL(Sys.Getenv("jolts_openings")), final, overwrite = TRUE)

dbDisconnect(con)
