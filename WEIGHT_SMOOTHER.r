.libPaths(c("/home/devlin/R/aarch64-unknown-linux-gnu-library/4.2",
            "/usr/local/lib/R/site-library",
            "/usr/lib/R/site-library",
            "/usr/lib/R/library"))

library(tidyverse)
library(zoo)
library(RPostgres)
library(DBI)

print('--BEGIN WEIGHT_SMOOTHER.R--')

con <- dbConnect(drv = Postgres(),
                 dbname = Sys.getenv("db_home"),
                 host = Sys.getenv("db_ip"),
                 port = as.numeric(Sys.getenv("db_port")),
                 user = Sys.getenv("db_user"),
                 password = Sys.getenv("db_password"))

NOW <- today()
START <- NOW - months(2)

START_INT <- str_c(year(START),
                   str_pad(month(START), 2, 'left', '0'),
                   '01') %>% 
  as.numeric()

qry <- str_c("SELECT * FROM prd.weight WHERE CAST(tpd AS INTEGER) >= ", START_INT)

monthly_weights <- dbGetQuery(con, qry) %>%
  mutate(tpd = as_date(tpd)) %>%
  complete(tpd = full_seq(tpd, 1)) %>%
  arrange(tpd) %>%
  mutate(tod = ifelse(is.na(tod), "computed", tod),
         tpd = format(tpd, "%Y%m%d"),
         weight = na.approx(weight, na.rm = FALSE))

qry <- str_c("DELETE FROM prd.weight WHERE CAST(tpd AS INTEGER) >= ", START_INT)
dbExecute(con, qry)

dbWriteTable(con, SQL("prd.weight"), monthly_weights, append = TRUE, row.names = FALSE)

dbDisconnect(con)
print('--END WEIGHT_SMOOTHER.R--')



