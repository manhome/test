library(XML)
library(RSQLite)
library(tidyverse)
library(readxl)
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

get_return <- function(dbconn, sec_id, start_date, end_date)
{
    if (missing(sec_id)) sec_id <- NULL
    if (missing(start_date)) start_date <- as.Date("1970-01-01")
    if (missing(end_date)) end_date <- as.Date("2100-01-01")

    tmp_start_date <- as.numeric(as.POSIXct(start_date, tz="UTC"))
    tmp_end_date <- as.numeric(as.POSIXct(end_date, tz="UTC"))

    tmp_sql <- "SELECT * FROM sec_return
                WHERE [Date] >= ? AND [Date] <= ?"

    if (length(sec_id) > 0) {
        tmp_param_id <- str_flatten_comma(sec_id)
        tmp_sql <- paste(tmp_sql, "AND sec_id IN (", tmp_param_id, ")")
    }

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

    if (length(sec_id) > 0) {
        tmp_dfPV_chg <- get_return(dbconn, sec_id)
        tmp_dfP_last <- get_last_price(dbconn, sec_id)
        tmp_dfV_last <- get_last_volume(dbconn, sec_id)
    } else {
        tmp_dfPV_chg <- get_return(dbconn)
        tmp_dfP_last <- get_last_price(dbconn)
        tmp_dfV_last <- get_last_volume(dbconn)
    }

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

#qMain <- function(chrFileParam, dtStart, dtEnd)
qMain <- function()
{
    dbconn <<- dbConnect(RSQLite::SQLite(), "", flags=SQLITE_RW)

    on.exit(
        tryCatch(
            dbDisconnect(dbconn),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
        })
    )

    tmp_start_date_default <- as.Date("1999-12-31")
    file_stock_list <- "HK.xlsx"
    tmp_file_path <- "C:/App/R/StockData"
    tmp_file_db <- c("hk_stock_return.db", "hk_stock_price.db")
    tmp_file_db <- file.path(tmp_file_path, tmp_file_db)
    db_alias <- str_replace(basename(tmp_file_db), pattern="([.]db)$", "")

    for(i in 1:length(tmp_file_db)) {
        tmp_sql <- sprintf("ATTACH DATABASE '%s' AS %s", tmp_file_db[i], db_alias[i])
        cat(sprintf("\t%s\n", tmp_sql))
        rs <- dbSendStatement(dbconn, tmp_sql)
        dbClearResult(rs)
    } # for (db

    # Update Table Stocks
    tmp_sheets <- c("stock", "etf")

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

#    k1 <- c(1:20, 260, 400)
#    n <- length(k1)
#    for (i in k1) {

    tmp_df_all <- NULL

    n <- nrow(tmp_dfMaxDate)
    for (i in 1:n) {
        if (i %% 100 < 1) cat(sprintf("\t%.2f pct completed.\n", 100*i/n))
        tmp_sec_id <- tmp_dfMaxDate$sec_id[i]
        tmp_id_yahoo <- tmp_dfMaxDate$id_yahoo[i]
        tmp_start_date <- format(tmp_dfMaxDate$Date[i], "%Y-%m-%d")
        tmp_end_date <- format(Sys.Date(), "%Y-%m-%d")

        cat(sprintf("\tProcess %s\n", tmp_id_yahoo))
        tmp_df <- tq_get(tmp_id_yahoo, from=tmp_start_date, to=tmp_end_date)
        if (is.data.frame(tmp_df) && (nrow(tmp_df) > 0)) {
            tmp_df_all <- bind_rows(tmp_df_all, tmp_df)
        }
    } # for (i

#    dbWriteTable(dbconn, "tmp_test", tmp_df_all, overwrite=TRUE, temporary=FALSE)

    tmp_var_price <- c("open", "high", "low", "close", "adj_close")
    tmp_var_volume <- c("volume")

    tmp_df_1 <- tmp_df_all %>%
                select(symbol, date) %>%
                group_by(symbol) %>%
                summarize(start_date=min(date, na.rm=TRUE),
                    end_date=max(date, na.rm=TRUE)) %>%
                ungroup()

    tmp_df_dates <- NULL
    for (i in 1:nrow(tmp_df_1)) {
        tmp_symbol <- tmp_df_1$symbol[i]
        tmp_start_date <- tmp_df_1$start_date[i]
        tmp_end_date <- tmp_df_1$end_date[i]
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

    # tmp_dfData <- tmp_dfData %>%
    #                 group_by(id_yahoo) %>%
    #                 arrange(Date, .by_group=TRUE) %>%
    #                 fill(any_of(tmp_var_price), .direction="down") %>%
    #                 ungroup()

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

    qdb_sec_price_update(dbconn)

    tryCatch(
        dbDisconnect(dbconn),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
    })
    #save(list=c("tmp_df_all", "tmp_dfMaxDate"), envir=pos.to.env(1), file="test.RData")

}

###########
# NOT USED
###########
get_last_date <- function(dbconn)
{
    tmp_sql <- "SELECT a1.sec_id,
                IFNULL((SELECT MAX(a2.Date) FROM sec_return a2
                    WHERE a2.sec_id = a1.sec_id), 0) AS [last_return_date],
                IFNULL((SELECT MAX(a2.Date) FROM sec_price a2
                    WHERE a2.sec_id = a1.sec_id), 0) AS [last_price_date],
                IFNULL((SELECT a2.close FROM sec_price a2
                    WHERE a2.sec_id = a1.sec_id
                    AND a2.Date = (SELECT MAX(a3.Date) FROM sec_price a3
                        WHERE a3.sec_id = a1.sec_id)), 0) AS [last_close]
            FROM securities a1
            WHERE EXISTS(SELECT 1 FROM last_price a2
                WHERE a2.sec_id = a1.sec_id)"
    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")

    tmp_df <- dbGetQuery(dbconn, tmp_sql)
    tmp_df$last_return_date <- as.Date.POSIXct(tmp_df$last_return_date)
    tmp_df$last_price_date <- as.Date.POSIXct(tmp_df$last_price_date)

    return (tmp_df)
}

