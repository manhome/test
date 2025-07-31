library(XML)
library(RSQLite)
library(tidyverse)
library(tidyquant)
library(openxlsx)

source("qDBSQLite.R", local=TRUE, echo=FALSE)
source("qDBSQLite_utility.R", local=TRUE, echo=FALSE)

lag_cum_chg <- function(x) {
    x[is.na(x)] <- 0
    y <- cumprod(1/(1+x))
    y <- lag(y)
    y[is.na(y)] <- 1
    return (y)
}

lag_cum_chg_na <- function(x) {
    y <- lag_cum_chg(x)
    y[is.na(x)] <- NA
    return (y)
}

pct_chg <- function(x) {
    # assign NA to 0 as well
    x[which(x==0)] <- NA
    x0 <- lag(na.locf(x, na.rm=FALSE))
    y <- (x - x0) / abs(x0)
    y[is.infinite(y)|is.nan(y)] <- NA
    return (y)
}

get_max_date <- function(dbconn, start_date)
{
    if (missing(start_date)) start_date <- Sys.Date()
    tmp_start_date <- as.numeric(as.POSIXct(start_date, tz="UTC"))

    tmp_sql <- "SELECT s1.sec_id, s1.id_yahoo, 
                IFNULL(
                    (SELECT MAX(a1.Date) FROM sec_return a1
                        WHERE a1.sec_id = s1.sec_id
                        AND a1.volume != 0
                        AND a1.Date < (SELECT MAX(a2.Date) FROM sec_return a2
                            WHERE a2.sec_id = a1.sec_id
                            AND a2.volume != 0
                            AND a2.Date < ?)), ?) AS [Date]
            FROM securities s1
            ORDER BY [sec_id]"

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")

    tmp_params <- as.list(rep(tmp_start_date, 2))

    tmp_df <- dbGetQuery(dbconn, tmp_sql, params=tmp_params)
    tmp_df$Date <- as.Date.POSIXct(tmp_df$Date)

    return (tmp_df)
}

get_sec_return <- function(dbconn, sec_id, start_date, end_date)
{
    if (missing(sec_id)) sec_id <- NULL
    if (missing(start_date)) start_date <- as.Date("1970-01-01")
    if (missing(end_date)) end_date <- as.Date("2099-12-31")

    tmp_start_date <- as.numeric(as.POSIXct(start_date, tz="UTC"))
    tmp_end_date <- as.numeric(as.POSIXct(end_date, tz="UTC"))

    tmp_sql <- "SELECT * FROM sec_return r1"

    if (length(sec_id) > 0) {
        tmp_param_id <- str_flatten_comma(sec_id)
        tmp_sql <- paste(tmp_sql, "WHERE sec_id IN (", tmp_param_id, ")")
    } else {
        tmp_sql <- paste(tmp_sql, "WHERE sec_id IS NOT NULL")
    }

    # return start_date is at least 1 day with non-zero volume before
    tmp_sql_1 <- "AND [Date] >= IFNULL(
            (SELECT MAX(r2.Date) FROM sec_return r2
            WHERE r2.sec_id = r1.sec_id
            AND r2.volume != 0
            AND r2.Date < ?), 0)"
    tmp_sql_2 <- "AND [Date] <= ?"

    tmp_sql <- paste(tmp_sql, tmp_sql_1, tmp_sql_2)

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")

    tmp_params <- as.list(c(tmp_start_date, tmp_end_date))

    tmp_df <- dbGetQuery(dbconn, tmp_sql, params=tmp_params)
    tmp_df$Date <- as.Date.POSIXct(tmp_df$Date)

    return (tmp_df)
}

get_last_price <- function(dbconn, sec_id)
{
    if (missing(sec_id)) sec_id <- NULL

    tmp_sql <- "SELECT * FROM last_price"

    if (length(sec_id) > 0) {
        tmp_param_id <- str_flatten_comma(sec_id)
        tmp_sql <- paste(tmp_sql, "WHERE sec_id IN (", tmp_param_id, ")")
    }

    tmp_df <- dbGetQuery(dbconn, tmp_sql)
    tmp_df$Date <- as.Date.POSIXct(tmp_df$Date)

    return (tmp_df)
}

get_last_volume <- function(dbconn, sec_id)
{
    if (missing(sec_id)) sec_id <- NULL

    tmp_sql <- "SELECT * FROM last_volume"

    if (length(sec_id) > 0) {
        tmp_param_id <- str_flatten_comma(sec_id)
        tmp_sql <- paste(tmp_sql, "WHERE sec_id IN (", tmp_param_id, ")")
    }

    tmp_df <- dbGetQuery(dbconn, tmp_sql)
    tmp_df$Date <- as.Date.POSIXct(tmp_df$Date)

    return (tmp_df)
}

get_sec_id <- function(dbconn)
{
    tmp_sql <- "SELECT sec_id FROM last_price t1
        WHERE t1.Date > (SELECT MAX(t2.Date) FROM sec_price t2 
            WHERE t2.sec_id = t1.sec_id)
        UNION SELECT sec_id 
        FROM last_volume t1
        WHERE t1.Date > (SELECT MAX(t2.Date) FROM sec_price t2 
            WHERE t2.sec_id = t1.sec_id AND t2.volume > 0)"

    tmp_df <- dbGetQuery(dbconn, tmp_sql)

    return (tmp_df$sec_id)
}

qdb_table_update <- function(dbconn, tmp_dfData, table_name, keys)
{
    tmp_chrTmpTable <- paste0("tmp_db_", table_name)
    source_tbl <- tmp_chrTmpTable
    target_tbl <- table_name

    # convert any Date/POSIXt column into numeric unix timestamp first
    tmp_df <- tmp_dfData %>%
                mutate(across(where(lubridate::is.Date), ~as.POSIXct(.x, tz="UTC"))) %>%
                mutate(across(where(lubridate::is.POSIXt), ~as.numeric(.x)))

    rc <- create_temp_table(dbconn, tmp_df, tmp_chrTmpTable, keys)
    rc <- table_update(dbconn, target_tbl, source_tbl)
}

qdb_sec_price_update <- function(dbconn, sec_id)
{
    if (missing(sec_id)) sec_id <- NULL

    tmp_var_price <- c("open", "high", "low", "close", "adj_close")
    tmp_var_volume <- c("volume")

    tmp_dfPV_chg <- get_sec_return(dbconn, sec_id)
    tmp_dfP_last <- get_last_price(dbconn, sec_id)
    tmp_dfV_last <- get_last_volume(dbconn, sec_id)

    tmp_dfPV <- tmp_dfPV_chg %>%
        left_join(tmp_dfP_last, by=c("sec_id", "Date"), suffix=c("", "_last")) %>%
        left_join(tmp_dfV_last, by=c("sec_id", "Date"), suffix=c("", "_last")) %>%
        group_by(sec_id) %>%
        arrange(desc(Date), .by_group=TRUE) %>%
        fill(ends_with("_last"), .direction="down") %>%
        mutate(
            across(any_of(tmp_var_price), ~lag_cum_chg_na(.)),
            across(any_of(tmp_var_volume), ~lag_cum_chg_na(.)),
            ) %>%
        mutate(
            open=round(open_last*open, 6),
            high=round(high_last*high, 6),
            low=round(low_last*low, 6),
            close=round(close_last*close, 6),
            volume=round(volume_last*volume, 6),
            adj_close=round(adj_close_last*adj_close, 6),
        ) %>%
        select(sec_id, Date, any_of(tmp_var_price), any_of(tmp_var_volume)) %>%
        arrange(sec_id, Date)

    if (nrow(tmp_dfPV) == 0) {
        cat(sprintf("\tNo derived Price/Volume for {%s}\n", sec_id))
        return (-1)
    }

    keys <- c("sec_id")
    table_name <- "sec_price"
    qdb_table_update(dbconn, tmp_dfPV, table_name, keys)

    return (0)
}

qMain <- function(file_config, dtStart, dtEnd)
{
    dbconn <<- dbConnect(RSQLite::SQLite(), "", flags=SQLITE_RW)

    on.exit(
        tryCatch(
            dbDisconnect(dbconn),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
        })
    )

    # tmp_start_date_default <- as.Date("1999-12-31")
    # file_stock_list <- "HK.xlsx"
    # tmp_file_path <- "C:/App/R/StockData"
    # tmp_file_db <- c("hk_stock_return.db", "hk_stock_price.db")
    # tmp_file_db <- file.path(tmp_file_path, tmp_file_db)

    tmp_config <- config::get(file=file_config)
    file_stock_list <- tmp_config$file_stock_list
    tmp_file_db <- tmp_config$file_db
    tmp_start_date_default <- as.Date(tmp_config$default_date)

    db_alias <- str_replace(basename(tmp_file_db), pattern="([.]db)$", "")
    for(i in 1:length(tmp_file_db)) {
        tmp_sql <- sprintf("ATTACH DATABASE '%s' AS %s", tmp_file_db[i], db_alias[i])
        cat(sprintf("\t%s\n", tmp_sql))
        rs <- dbSendStatement(dbconn, tmp_sql)
        dbClearResult(rs)
    } # for (db

    # Update Table Stocks
    # tmp_sheets <- c("stock", "etf")
    tmp_sheets <- getSheetNames(file_stock_list)

    # openxlsx::readWorkbook only accepts 1 sheet each time
    # Set simplify=FALSE to return a list of data.frame
    tmp_obj <- sapply(tmp_sheets, function(x) readWorkbook(file_stock_list, sheet=x),
                simplify=FALSE, USE.NAMES=FALSE)
    tmp_df <- do.call("rbind", tmp_obj)

    tmp_dfSecs <- tmp_df %>%
                    distinct(sec_id, .keep_all=TRUE)

    keys <- "sec_id"
    table_name <- "securities"
    qdb_table_update(dbconn, tmp_dfSecs, table_name, keys)

    # get last date for each stock return
    tmp_dfMaxDate <- get_max_date(dbconn, tmp_start_date_default)

    tmp_df_all <- NULL

#    k1 <- c(1:20, 602, 846)
#    n <- length(k1)
#    for (i in k1) {
    n <- nrow(tmp_dfMaxDate)
    for (i in 1:n) {
        if (i %% 100 < 1) cat(sprintf("\t%.2f pct completed.\n", 100*i/n))
        tmp_sec_id <- tmp_dfMaxDate$sec_id[i]
        tmp_id_yahoo <- tmp_dfMaxDate$id_yahoo[i]
        tmp_start_date <- format(max(c(tmp_dfMaxDate$Date[i], dtStart), na.rm=TRUE), "%Y-%m-%d")
        tmp_end_date <- format(min(c(Sys.Date(), dtEnd), na.rm=TRUE), "%Y-%m-%d")

        cat(sprintf("\tProcess %s\n", tmp_id_yahoo))
        tmp_df <- tq_get(tmp_id_yahoo, from=tmp_start_date, to=tmp_end_date)
        if (is.data.frame(tmp_df) && (nrow(tmp_df) > 0)) {
            tmp_df_all <- bind_rows(tmp_df_all, tmp_df)
        }
    } # for (i

    tmp_var_price <- c("open", "high", "low", "close", "adj_close")
    tmp_var_volume <- c("volume")

    tmp_df <- tmp_df_all %>%
        select(symbol, date) %>%
        group_by(symbol) %>%
        summarize(start_date=min(date, na.rm=TRUE),
            end_date=max(date, na.rm=TRUE)) %>%
        ungroup()

    tmp_df_dates <- NULL
    for (i in 1:nrow(tmp_df)) {
        tmp_symbol <- tmp_df$symbol[i]
        tmp_start_date <- tmp_df$start_date[i]
        tmp_end_date <- tmp_df$end_date[i]
        tmp_dates <- seq.Date(from=tmp_start_date, to=tmp_end_date, by=1)
        tmp_df <- data.frame(symbol=tmp_symbol, date=tmp_dates, stringsAsFactors=FALSE)
        tmp_df_dates <- bind_rows(tmp_df_dates, tmp_df)
    } # for (i

    tmp_dfData <- tmp_df_all %>%
        right_join(tmp_df_dates, by=c("symbol", "date"), suffix=c("", "")) %>%
        rename(id_yahoo=symbol, Date=date, adj_close=adjusted) %>%
        mutate(across(any_of(c(tmp_var_price, tmp_var_volume)), ~replace_na(., 0))) %>%
        group_by(id_yahoo) %>%
        arrange(Date, .by_group=TRUE) %>%
        ungroup()

    tmp_dfData <- tmp_dfData %>%
        inner_join(tmp_dfMaxDate, by="id_yahoo", suffix=c("", "_tmp")) %>%
        select(sec_id, Date, any_of(tmp_var_price), any_of(tmp_var_volume))

    tmp_dfP <- tmp_dfData %>%
        select(sec_id, Date, any_of(tmp_var_price)) %>%
        group_by(sec_id) %>%
        arrange(Date, .by_group=TRUE) %>%
        mutate(across(any_of(tmp_var_price), ~pct_chg(.),
            .names="{.col}_chg")) %>%
        ungroup()

    tmp_dfV <- tmp_dfData %>%
        select(sec_id, Date, any_of(tmp_var_volume)) %>%
        group_by(sec_id) %>%
        arrange(Date, .by_group=TRUE) %>%
        mutate(across(any_of(tmp_var_volume), ~pct_chg(.),
            .names="{.col}_chg")) %>%
        ungroup()

    tmp_dfPV_chg <- tmp_dfP %>%
        left_join(tmp_dfV, by=c("sec_id", "Date")) %>%
        select(sec_id, Date, ends_with("_chg")) %>%
        rename_with(~str_replace(.x, "[_]chg", ""), .col=ends_with("_chg")) %>%
        filter(!is.na(close))

    tmp_dfP_max_date <- tmp_dfP %>%
        filter(close > 0) %>%
        group_by(sec_id) %>%
        summarize(Date=max(Date, na.rm=TRUE)) %>%
        ungroup()

    tmp_dfV_max_date <- tmp_dfV %>%
        filter(volume > 0) %>%
        group_by(sec_id) %>%
        summarize(Date=max(Date, na.rm=TRUE)) %>%
        ungroup()

    tmp_dfP_last <- tmp_dfP %>%
        inner_join(tmp_dfP_max_date, by=c("sec_id", "Date")) %>%
        select(sec_id, Date, any_of(tmp_var_price))

    tmp_dfV_last <- tmp_dfV %>%
        inner_join(tmp_dfV_max_date, by=c("sec_id", "Date")) %>%
        select(sec_id, Date, any_of(tmp_var_volume))

    keys <- "sec_id"
    table_name <- "last_price"
    qdb_table_update(dbconn, tmp_dfP_last, table_name, keys)

    keys <- "sec_id"
    table_name <- "last_volume"
    qdb_table_update(dbconn, tmp_dfV_last, table_name, keys)

    keys <- c("sec_id", "Date")
    table_name <- "sec_return"
    qdb_table_update(dbconn, tmp_dfPV_chg, table_name, keys)

    tmp_sec_id <- get_sec_id(dbconn)
    if (length(tmp_sec_id) > 0) {
        for (sec_id in tmp_sec_id) {
            rc <- qdb_sec_price_update(dbconn, sec_id=sec_id)
            if (rc == 0) {
                cat(sprintf("\tUpdate sec_price for sec_id = %s\n", sec_id))
            } else {
                cat(sprintf("\tFail to update sec_price for sec_id = %s\n", sec_id))
            }
        } # for
   }

    tryCatch(
        dbDisconnect(dbconn),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
    })

    return (0)

}

#######
# Main
#######
# trailingOnly=TRUE means that only arguments after --args are returned
# if trailingOnly=FALSE then you got:
# [1] "--no-restore" "--no-save" "--args" "2010-01-28" "example" "100"
args <- commandArgs(trailingOnly=TRUE)

if (length(args) == 0) {
    print("Invalid command. e.g. <prompt>command.bat param.xml [YYYYMMDD] [YYYYMMDD]")
    quit(save="no", status=-1)
} # if (length(args

dtStart <- as.Date("1970-01-01")
dtEnd <- as.Date("2099-12-31")

file_config <- args[1]

if (length(args) > 1) {
    chrStartDate <- args[2]
    dtStart <- as.Date(chrStartDate, "%Y%m%d")
} # if (length(args

if (length(args) > 2) {
    chrEndDate <- args[3]
    dtEnd <- as.Date(chrEndDate, "%Y%m%d")
} # if (length(args

rc <- qMain(file_config, dtStart, dtEnd)
if (rc != 0) {
    quit(save="no", status=rc)
} # if (rc

test_debug <- function()
{
    # tmp_dfData <- tmp_dfData %>%
    #                 group_by(id_yahoo) %>%
    #                 arrange(Date, .by_group=TRUE) %>%
    #                 fill(any_of(tmp_var_price), .direction="down") %>%
    #                 ungroup()

    #dbWriteTable(dbconn, "tmp_test", tmp_df_all, overwrite=TRUE, temporary=FALSE)
    #save(list=c("tmp_df_all", "tmp_dfMaxDate"), envir=pos.to.env(1), file="test.RData")
}
