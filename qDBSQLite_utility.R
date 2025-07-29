library(tidyverse)
library(odbc)
library(DBI)
library(RSQLite)

################################################################################
# construct_sql_delete
# construct_sql_insert
# construct_sql_remove_duplicate
# create_temp_table
# enum_table_update
# execute_tran
# get_enum_table_names
# get_fields
# get_keys
# get_null_fields
# replace_enum_fields
# select_sql
# table_update
#
################################################################################
construct_sql_delete <- function(target_tbl, source_tbl, target_keys)
{
    if (missing(target_keys)) {
        target_keys <- get_keys(dbconn, target_tbl)
    } #if

    if (length(target_keys) > 0) {
        tmp_target_keys <- paste(target_tbl, target_keys, sep=".")
        tmp_source_keys <- paste(source_tbl, target_keys, sep=".")
        tmp_key_criteria <- paste0(paste0(tmp_source_keys, " = "),
                                   tmp_target_keys, collapse=" AND ")
    } else {
        tmp_key_criteria <- "1 = 1"
    } # if

    tmp_sql <- "DELETE FROM %target_tbl
                WHERE EXISTS(
                    SELECT 1 FROM %source_tbl
                    WHERE %key_criteria)"

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_sql <- str_replace_all(tmp_sql, "%target_tbl", target_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%source_tbl", source_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%key_criteria", tmp_key_criteria)

    return (tmp_sql)
}

construct_sql_insert <- function(target_tbl, source_tbl, target_fields, target_keys)
{
    if (missing(target_fields)) {
        target_fields <- get_fields(dbconn, target_tbl)
    } #if

    if (missing(target_keys)) {
        target_keys <- get_keys(dbconn, target_tbl)
    } #if

    tmp_target_flds <- paste(target_fields, collapse=", ")

    tmp_target_keys <- paste(target_tbl, target_keys, sep=".")
    tmp_source_keys <- paste(source_tbl, target_keys, sep=".")
    tmp_key_criteria <- paste0(paste0(tmp_target_keys, " = "), tmp_source_keys, collapse=" AND ")
    tmp_key_not_null <- paste0(paste0(tmp_source_keys, " IS NOT NULL"), collapse=" AND ")

    tmp_sql <- "INSERT INTO %target_tbl
                (%target_flds)
                SELECT DISTINCT %target_flds FROM %source_tbl
                WHERE %key_not_null
                AND NOT EXISTS(
                    SELECT 1 FROM %target_tbl
                    WHERE %key_criteria)"

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_sql <- str_replace_all(tmp_sql, "%target_tbl", target_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%source_tbl", source_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%target_flds", tmp_target_flds)
    tmp_sql <- str_replace_all(tmp_sql, "%key_criteria", tmp_key_criteria)
    tmp_sql <- str_replace_all(tmp_sql, "%key_not_null", tmp_key_not_null)

    return (tmp_sql)
}

construct_sql_remove_duplicate <- function(target_tbl, key_ordinal, check_fields, key_fields)
{
    if (missing(check_fields)) {
        target_fields <- get_fields(dbconn, target_tbl)
        check_fields <- setdiff(target_fields, key_ordinal)
        null_fields <- get_null_fields(dbconn, target_tbl)
    } # if

    if (missing(key_fields)) {
        target_keys <- get_keys(dbconn, target_tbl)
        # other key fields except date to identify records
        key_fields <- setdiff(target_keys, key_ordinal)
    } # if

    if (length(check_fields) > 0) {
        tmp_check_fields <- paste(target_tbl, check_fields, sep=".")
        tmp_source_check_fields <- paste("t2", check_fields, sep=".")

        # check whether fields are nullable. need additional treatment before comparison.
        # IFNULL(field, '') = IFNULL(field, '')
        tmp_check_fields_ifnull <- sprintf(ifelse(check_fields %in% null_fields,
                                    "IFNULL(%s, '')", "%s"), tmp_check_fields)

        tmp_source_check_fields_ifnull <- sprintf(ifelse(check_fields %in% null_fields,
                                    "IFNULL(%s, '')", "%s"), tmp_source_check_fields)

        tmp_check_criteria <- paste0(paste0(tmp_source_check_fields_ifnull, " = "),
                                     tmp_check_fields_ifnull, collapse=" AND ")
        tmp_check_criteria <- paste0(tmp_check_criteria, " AND ")
    } else {
        tmp_check_criteria <- ""
    } # if

    if (length(key_fields) > 0) {
        tmp_target_key_fields <- paste(target_tbl, key_fields, sep=".")
        tmp_source_key_fields <- paste("t3", key_fields, sep=".")
        tmp_key_criteria <- paste0(paste0(tmp_source_key_fields, " = "),
                                tmp_target_key_fields, collapse=" AND ")
        tmp_key_criteria <- paste0(tmp_key_criteria, " AND ")
    } else {
        tmp_key_criteria <- ""
    } # if

    tmp_sql <- "DELETE FROM %target_tbl
                WHERE EXISTS(SELECT 1 FROM %target_tbl t2
                    WHERE %check_criteria
                    t2.%key_ordinal = (SELECT MAX(t3.%key_ordinal) FROM %target_tbl t3
                        WHERE %key_criteria
                        t3.%key_ordinal < %target_tbl.%key_ordinal))"

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_sql <- str_replace_all(tmp_sql, "%target_tbl", target_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%check_criteria", tmp_check_criteria)
    tmp_sql <- str_replace_all(tmp_sql, "%key_criteria", tmp_key_criteria)
    tmp_sql <- str_replace_all(tmp_sql, "%key_ordinal", key_ordinal)

    return (tmp_sql)
}

create_temp_table <- function(dbconn, dfData, tbl_name, keys)
{
    # always remove grouping before mutate(across...
    dfData <- dfData %>% ungroup()
#        mutate(across(where(lubridate::is.Date), ~format(.x, "%Y-%m-%d"))) %>%
#        mutate(across(where(lubridate::is.POSIXt), ~format(.x, "%Y-%m-%d %H:%M:%OS")))

    cat(sprintf("\tCreate Temporary Table %s\n", tbl_name))

    dbWriteTable(dbconn, tbl_name, dfData, overwrite=TRUE, temporary=TRUE)

    if (!missing(keys)) {
        tmp_keys <- paste(keys, collapse=", ")
        cat(sprintf("\tCreate Index %s\n", tbl_name))
        tmp_sql <- "CREATE INDEX ix_%s ON %s (%s)"
        tmp_sql <- sprintf(tmp_sql, tbl_name, tbl_name, tmp_keys)

        rc <- tryCatch(
                qDBSQLite_SendStatement(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (-1)
                })
        if (rc!=0) {
            return (-1)
        } # if

        # temp table with single field distinct value to speed up deletion
        for (i in 1:length(keys)) {
            tmp_key <- keys[i]
            tmp_tbl_name <- paste(tbl_name, tmp_key, sep="_")
            tmp_df <- dfData %>%
                select(any_of(tmp_key)) %>%
                distinct()

            cat(sprintf("\tCreate Temporary Table %s\n", tmp_tbl_name))
            dbWriteTable(dbconn, tmp_tbl_name, tmp_df, overwrite=TRUE, temporary=TRUE)
        } # for
    } # if

    return (0)
}

enum_table_update <- function(dbconn, target_tbl, source_tbl, source_field)
{
    get_next_id <- function(dbconn, tbl_name)
    {
        tmp_sql <- "SELECT IFNULL(MAX(id),0)+1 FROM %s"
        tmp_sql <- sprintf(tmp_sql, tbl_name)
        tmp_df <- tryCatch(
                    dbGetQuery(dbconn, tmp_sql),
                    error=function(e) {
                        cat(sprintf("\t%s\n", e))
                    })
        return (tmp_df[[1]])
    }

    # target_tbl always has the columns {id, value}
    tmp_sql <- "SELECT DISTINCT %source_field AS value
                FROM %source_tbl t1
                WHERE %source_field IS NOT NULL
                AND NOT EXISTS(SELECT 1 FROM %target_tbl t2
                    WHERE t2.value = t1.%source_field) ORDER BY 1"
    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_sql <- str_replace_all(tmp_sql, "%target_tbl", target_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%source_tbl", source_tbl)
    tmp_sql <- str_replace_all(tmp_sql, "%source_field", source_field)

    rc <-  select_sql(dbconn, tmp_sql)
    if (!is.data.frame(rc) || nrow(rc)==0) {
        cat(sprintf("\tNo new entry to table '%s'\n", target_tbl))
        return (0)
    }

    cat(sprintf("\tUpdate enumerate table '%s'\n", target_tbl))
    tmp_values <- rc[[1]]
    # give quotation to string if necessary
    tmp_values <- dbQuoteLiteral(dbconn, tmp_values)

    tmp_sql <- "INSERT INTO %target_tbl (id, value) VALUES(%s, %s)"
    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_sql <- str_replace_all(tmp_sql, "%target_tbl", target_tbl)

    for (i in 1:length(tmp_values)) {
        tmp_id <- get_next_id(dbconn, target_tbl)
        tmp_value <- tmp_values[i]
        tmp_sql_1 <- sprintf(tmp_sql, tmp_id, tmp_value)
        rc <- execute_tran(dbconn, tmp_sql_1)
        if (rc != 0) {
            return (-1)
        } # if
    } # for

    return (0)
}

enum_tables_update <- function(dbconn, source_tbl)
{
    cat(sprintf("\tUpdate Enumerate Tables from %s (begin)\n", source_tbl))
    tmp_enum_names <- get_enum_table_names(dbconn)
    tmp_fields <- get_fields(dbconn, source_tbl)

    j <- match(str_replace(tmp_enum_names, "^(enum)[_]", ""), tmp_fields)
    tmp_enum <- tmp_enum_names[!is.na(j)]
    tmp_colnum <- na.omit(j)

    if (length(tmp_enum) > 0) {
        for (i in 1:length(tmp_enum)) {
            tmp_colnum_i <- tmp_colnum[i]
            tmp_enum_i <- tmp_enum[i]
            tmp_source_fld <- tmp_fields[tmp_colnum_i]
            rc <- enum_table_update(dbconn, tmp_enum_i, source_tbl, tmp_source_fld)
            if (rc!=0) {
                cat(sprintf("\tError when updating Enumerate Table '%s'\n", tmp_enum_i))
            } # if
        } # for
    } # if
    cat(sprintf("\tUpdate Enumerate Tables from %s (end)\n", source_tbl))
    return (0)
}

execute_tran <- function(dbconn, sqls, verbose=FALSE)
{
    dbBegin(dbconn)

    for (i in 1:length(sqls)) {
        tmp_sql <- sqls[i]
        tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")

        if (verbose == TRUE) {
            cat(sprintf("\tExecute SQL '%s'\n", tmp_sql))
        } else {
            #return first 15 words only
            cat(sprintf("\tExecute SQL '%s'\n",
                paste(na.omit(unlist(strsplit(tmp_sql, split = "\\s+"))[1:15]), collapse=" ")))
        } # if

        rc <- tryCatch(
                qDBSQLite_SendStatement(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (-1)
                })
        if (rc!=0) {
            dbRollback(dbconn)
            return (-1)
        } # if
    } # for (i

    dbCommit(dbconn)
    return (0)
}

get_enum_table_names <- function(dbconn)
{
    tmp_sql <- "SELECT * FROM PRAGMA_table_list()
                WHERE type = 'table' AND ncol >= 2 AND name LIKE 'enum_%'
                ORDER BY name"

    tmp_sql <- str_replace_all(tmp_sql, "\\s+", " ")
    tmp_df <- tryCatch(
                dbGetQuery(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })
    if (is.data.frame(tmp_df)) {
        tmp_names <- tmp_df$name
    } else {
        tmp_names <- c()
    }
    return (tmp_names)
}

get_fields <- function(dbconn, tbl_name)
{
    tmp_sql <- "SELECT name FROM pragma_table_info('%s') ORDER BY cid"
    tmp_sql <- sprintf(tmp_sql, tbl_name)
    tmp_df <- tryCatch(
                dbGetQuery(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })
    if (is.data.frame(tmp_df)) {
        tmp_names <- tmp_df$name
    } else {
        tmp_names <- c()
    }
    return (tmp_names)
}

get_keys <- function(dbconn, tbl_name)
{
    tmp_sql <- "SELECT name FROM pragma_table_info('%s') WHERE pk > 0 ORDER BY pk"
    tmp_sql <- sprintf(tmp_sql, tbl_name)
    tmp_df <- tryCatch(
                dbGetQuery(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })
    if (is.data.frame(tmp_df)) {
        tmp_names <- tmp_df$name
    } else {
        tmp_names <- c()
    }
    return (tmp_names)
}

get_null_fields <- function(dbconn, tbl_name)
{
    tmp_sql <- "SELECT name FROM PRAGMA_table_info('%s') WHERE [notnull] = 0"
    tmp_sql <- sprintf(tmp_sql, tbl_name)
    tmp_df <- tryCatch(
                dbGetQuery(dbconn, tmp_sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                })
    if (is.data.frame(tmp_df)) {
        tmp_names <- tmp_df$name
    } else {
        tmp_names <- c()
    }
    return (tmp_names)
}

replace_enum_fields <- function(dbconn, dfData)
{
    tmp_df <- dfData

    tmp_enum_names <- get_enum_table_names(dbconn)
    j <- match(str_replace(tmp_enum_names, "^(enum)[_]", ""), names(tmp_df))

    tmp_enum <- tmp_enum_names[!is.na(j)]
    tmp_colnum <- na.omit(j)

    if (length(tmp_enum)==0) {
        cat(sprintf("\tNo column to replace.\n"))
        return (tmp_df)
    } # if

    for (i in 1:length(tmp_enum)) {
        tmp_colnum_i <- tmp_colnum[i]
        tmp_enum_i <- tmp_enum[i]

        cat(sprintf("\tReplace column '%s' with enumerate value.\n", names(tmp_df)[tmp_colnum_i]))
        tmp_sql <- "SELECT * FROM %s ORDER BY 1"
        tmp_sql <- sprintf(tmp_sql, tmp_enum_i)
        tmp_df_enum <- select_sql(dbconn, tmp_sql)

        tmp_enum_id <- tmp_df_enum[[1]]
        tmp_enum_value <- tmp_df_enum[[2]]

        k <- match(tmp_df[[tmp_colnum_i]], tmp_enum_value)
        tmp_df[[tmp_colnum_i]] <- tmp_enum_id[k]
    } # for (i

    return (tmp_df)
}

select_sql <- function(dbconn, sql)
{
    tmp_df <- tryCatch(
                dbGetQuery(dbconn, sql),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (NULL)
                })
    return (tmp_df)
}

table_update <- function(dbconn, target_tbl, source_tbl, key_ordinal)
{
    cat(sprintf("\t\tUpdate Table %s (begin)\n", target_tbl))

    tmp_sqls <- c();

    if (missing(key_ordinal)) {
        # use raw source table instead
        tmp_source_tbl <- source_tbl
        tmp_sql <- construct_sql_delete(target_tbl, tmp_source_tbl)
        tmp_sqls <- c(tmp_sqls, tmp_sql)
    } else {
        if (length(key_ordinal)==1) {
            # use simplified temporary source table instead
            tmp_source_tbl <- paste(source_tbl, key_ordinal, sep="_")
        } else {
            # use raw source table instead
            tmp_source_tbl <- source_tbl
        } #if

        tmp_sql <- construct_sql_delete(target_tbl, tmp_source_tbl, key_ordinal)
        tmp_sqls <- c(tmp_sqls, tmp_sql)
    } # if

    tmp_sql <- construct_sql_insert(target_tbl, source_tbl)
    tmp_sqls <- c(tmp_sqls, tmp_sql)

    rc <- execute_tran(dbconn, tmp_sqls, verbose=TRUE)
    if (rc != 0) {
        cat(sprintf("\t\tFailed in execute_tran. Please check!\n"))
        return (-1)
    } # if

    cat(sprintf("\t\tUpdate Table %s (end)\n", target_tbl))
    return (0)
}
