.libPaths(c("/home/devlin/R/aarch64-unknown-linux-gnu-library/4.2",
            "/usr/local/lib/R/site-library",
            "/usr/lib/R/site-library",
            "/usr/lib/R/library"))

library(httr)
library(jsonlite)
library(tidyverse)
library(magrittr)
library(DBI)
library(RPostgres)

con <- dbConnect(drv = Postgres(),
                 dbname = Sys.getenv("db_sonr"),
                 host = Sys.getenv("db_ip"),
                 port = as.numeric(Sys.getenv("db_port"),
                 user = Sys.getenv("db_user"),
                 password = Sys.getenv("db_password"))

stringify_date <- function(date_object, dash = FALSE) {
  YR <- year(date_object)
  MO <- month(date_object)
  DY <- day(date_object)
  
  MO2 <- str_pad(MO, 2, "left", "0")
  DY2 <- str_pad(DY, 2, "left", "0")
  
  if (dash) {
    str_date <- str_c(YR, '-', MO2, '-', DY2)
  } else {
    str_date <- str_c(YR, MO2, DY2)
  }
  
  
  return (str_date)
  
}

table_update <- function(table_name, updater) {
  
  current_tbl <- dbGetQuery(con, str_c("SELECT * FROM ", table_name))
  
  history <- anti_join(current_tbl, updater, by = 'time_period')
  
  new_tbl <- bind_rows(history, updater)
  
  dbWriteTable(con, SQL(table_name), new_tbl, overwrite = TRUE)
  
}

###############################################################################\
##                                   ##\\\\\\\///////\            /\            ##  
##  ######  #####   ######  #####    ##\\\\\\\///////\\          /  \          /##
##  ##      ##  ##  ##      ##  ##   ##\\\\\\\///////\\\        /    \        / ##
##  #####   #####   #####   ##  ##   ##\\\\\\\///////\\\\      /      \      /  ##
##  ##      ##  ##  ##      ##  ##   ##\\\\\\\///////\\\\\    /        \    /   ##
##  ##      ##  ##  ######  #####    ##\\\\\\\///////\\\\\\  /          \  /    ##
##                                   ##\\\\\\\///////\\\\\\\/            \/     ##
###############################################################################/

fred_vars <- dbGetQuery(con, "SELECT * FROM api.fred_vars")

to_pull <- fred_vars %>%
  pull(series_id)

url_base <- "https://api.stlouisfed.org/fred/series/observations?"
url_key <- Sys.getenv("fred_api_key")


alldata <- tibble()

for (var_id in to_pull) {
  print(var_id)
  
  url_series <- str_c("series_id=", var_id)
  
  api_url <- str_c(url_base,
                   url_series,
                   url_key)
  
  res <- GET(api_url)
  
  data <- fromJSON(rawToChar(res$content))
  
  tbl <- data[["observations"]] %>%
    mutate(time_period = ymd(date))
  

  alldata <- tbl %>%
    mutate(series_id = var_id) %>%
    bind_rows(alldata)
  
}

ALLDATA <- alldata %>%
  select(series_id, time_period, value) %>%
  mutate(time_period = stringify_date(time_period)) %>%
  left_join(fred_vars)

dbWriteTable(con, SQL('api.fred'), ALLDATA, overwrite = TRUE)

################################################################################\
##                          ##\|||||||      /|\      |
##  ######  ######   ####   ##|\||||||     / | \     |
##  ##        ##    ##  ##  ##||\|||||    /  |  \    |
##  #####     ##    ######  ##|||\||||   /   |   \   |
##  ##        ##    ##  ##  ##||||\|||  /    |    \  |
##  ######  ######  ##  ##  ##|||||\|| /     |     \ |
##                          ##||||||\|/      |      \|
################################################################################/

api_key <- Sys.getenv("eia_api_key")
url_freq <- "&frequency=monthly"
url_wdw <- "&start=2024-01&end=2030-01"
url_oth <- "&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"


## Electric Retail Sales ==================================================== ##
url_base <- "https://api.eia.gov/v2/electricity/retail-sales/data/"
url_vars <- "&data[0]=customers&data[1]=price&data[2]=sales"
url_loc <- "&facets[stateid][]=FL"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_loc,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))

elec_pull <- data$response$data %>%
  mutate(across(customers:sales, ~as.numeric(.x)),
         period = stringify_date(ymd(str_c(period, '-01')))) %>%
  pivot_longer(customers:sales, names_to = 'metric', values_to = 'val') %>%
  rename(time_period = period) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_electric', elec_pull)

## Nat Gas Price ============================================================ ##

url_base <- "https://api.eia.gov/v2/natural-gas/pri/rescom/data/"
url_vars <- "&data[0]=value&facets[series][]=N3010FL3&facets[series][]=N3020FL3"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))

ngprc_pull <- data$response$data %>%
  mutate(value = as.numeric(value),
         period = stringify_date(ymd(str_c(period, '-01')))) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_natgasprice', ngprc_pull)

url_base <- "https://api.eia.gov/v2/natural-gas/cons/sum/data/"
url_vars <- "&data[0]=value&facets[series][]=N3010FL2&facets[series][]=N3020FL2"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))

ngcon_pull <- data$response$data %>%
  mutate(value = as.numeric(value),
         period = stringify_date(ymd(str_c(period, '-01')))) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_natgassales', ngcon_pull)

## Gas Prices =============================================================== ##

url_base <- "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
url_freq <- "&frequency=weekly"
url_vars <- "&data[0]=value&facets[duoarea][]=R1Z&facets[product][]=EPMR"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)


res = GET(api_url)

data = fromJSON(rawToChar(res$content))
gasolineprice <- data$response$data %>%
  mutate(period = stringify_date(ymd(period)),
         value = as.numeric(value)) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_padd1cprice', gasolineprice)

## Nat Gas in Storage ======================================================= ##

url_base <- "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"
url_vars <- "&data[0]=value&facets[series][]=NW2_EPG0_SWO_R48_BCF"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))
ngstorage <- data$response$data %>%
  mutate(period = stringify_date(ymd(period)),
         value = as.numeric(value)) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_natgasstorage', ngstorage)

## Nat Gas Withdrawls ======================================================= ##

url_base <- "https://api.eia.gov/v2/natural-gas/prod/sum/data/"
url_freq <- "&frequency=monthly"
url_vars <- "&data[0]=value&facets[process][]=FGW&facets[duoarea][]=NUS&facets[product][]=EPG0&facets[series][]=N9010US2"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))
ngoutput <- data$response$data %>%
  mutate(period = stringify_date(ymd(str_c(period, '-01'))),
         value = as.numeric(value)) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_natgasout', ngoutput)

## Pipeline Imports ========================================================= ##

url_base <- "https://api.eia.gov/v2/natural-gas/move/impc/data/"
url_freq <- "&frequency=monthly"
url_vars <- "&data[0]=value&facets[product][]=EPG0&facets[process][]=IRP&facets[series][]=N9102CN2"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))
pipelinein <- data$response$data %>%
  mutate(period = stringify_date(ymd(str_c(period, '-01'))),
         value = as.numeric(value)) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_pipelinein', pipelinein)

## Exports ================================================================== ##

url_base <- "https://api.eia.gov/v2/natural-gas/move/expc/data/"
url_vars <- "&data[0]=value&facets[process][]=ENG&facets[process][]=ENP&facets[process][]=EVE"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))
ngexports <- data$response$data %>%
  mutate(period = stringify_date(ymd(str_c(period, '-01'))),
         value = as.numeric(value)) %>%
  filter(units == 'MMCF') %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_ngexports', ngexports)

## Henry Hub Price of Natural Gas =========================================== ##

url_base <- "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
url_freq <- "&frequency=weekly"
url_vars <- "&data[0]=value&facets[series][]=RNGWHHD"

api_url <- str_c(url_base,
                 api_key,
                 url_freq,
                 url_vars,
                 url_wdw,
                 url_oth)

res = GET(api_url)

data = fromJSON(rawToChar(res$content))
henryhub <- data$response$data %>%
  mutate(period = stringify_date(ymd(period)),
         value = as.numeric(value)) %>%
  rename(time_period = period, val = value) %>%
  rename_with(~str_to_lower(.x)) %>%
  rename_with(~str_replace_all(.x, '-', '_'))

table_update('api.eia_henryhub', henryhub)

################################################################################\
##                          
##  ##   ##   #####  ######  ######
##  ###  ##  ##      ##        ##
##  ## # ##  ##      #####     ##
##  ##  ###  ##      ##        ##
##  ##   ##   #####  ######  ######
##
################################################################################/

ends <- today()
starts <- ends - years(1) + days(1)

api_key <- Sys.getenv("ncei_api_key")

base_url <- "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

vrs <- c('TMIN', 'TMAX', 'PRCP')

stations <- tibble(station = c('USW00013889', 'USW00012839', 'USW00012842', 
                               'USW00012815', 'USW00093805', 'USW00012816',
                               'USW00012894', 'USW00013899'),
                   city = c('jacksonville', 'miami', 'tampa', 
                            'orlando', 'tallahassee', 'gainesville',
                            'fort meyers', 'pensacola'))

output2 <- tibble()

for (ss in 1:length(starts)) {
  
  sdte <- stringify_date(starts[ss], dash = TRUE)
  edte <- stringify_date(ends[ss], dash = TRUE)
  
  print('--------------------------------------------')
  print(sdte)
  print('--------------------------------------------')
  
  for (vv in vrs) {
    
    print(vv)
    
    for (sid in stations$station) {
      print(sid)
      
      params <- list(
        datasetid = "GHCND",
        datatypeid = vv,
        stationid = str_c("GHCND:", sid),
        startdate = sdte,
        enddate = edte,
        units = "standard",
        limit = 1000
      )
      
      response <- GET(url = base_url, query = params, add_headers(token = api_key))
      
      sc <- status_code(response)
      
      tries <- 1
      
      while (sc != 200) {
        print('trying again')
        
        response <- GET(url = base_url, query = params, add_headers(token = api_key))
        
        sc <- status_code(response)
        
        tries <- tries + 1
        
        if (tries == 500) {
          
          print('too broken, sorry')
          
          break
          
        }
        
      }
      
      data <- content(response, as = "text")
      json_data <- fromJSON(data, flatten = TRUE)
      
      raw_set <- json_data$results %>%
        mutate(date = as_date(date),
               station = str_sub(station, 7, -1),
               var = vv) %>%
        select(obs_date = date, station, value, var)
      
      output2 <- bind_rows(output2, raw_set)
      
    }
    
    Sys.sleep(3)
    
  }
  
}

output5 <- output2 %>%
  left_join(stations) %>%
  pivot_wider(names_from = var, values_from = value) %>%
  mutate(avg_temp = (TMIN + TMAX) / 2) %>%
  arrange(city, obs_date) %>%
  fill(avg_temp) %>%
  mutate(time_period = stringify_date(obs_date)) %>%
  select(-obs_date)


table_update('api.ncei_select', output5)

dbDisconnect(con)
