library(tidyverse)
library(odbc)
library(DBI)
library(RSQLite)

################################################################################
# qDBSQLite_Compact
# qDBSQLite_sqlData
# qDBSQLite_SendStatement
################################################################################
qDBSQLite_Compact <- function(file_db)
{
    if (!file.exists(file_db)) {
        tmp_msg <- sprintf("No database file '%s'", file_db)
        stop(tmp_msg)
    }

    dbconn <- dbConnect(RSQLite::SQLite(), file_db)

    tmp_sql <- "VACUUM"
    rc <- tryCatch(
            qDBSQLite_SendStatement(dbconn, tmp_sql),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
            })

    dbDisconnect(dbconn)
}

################################################################################
# Function: qDBSQLite_sqlData
# Description:
# Translate R data type to Access data type for query
# sqlData in odbc package cannot be used in MS Access
# Parameters:
#	(1) dbconn = database connection
#   (2) dfData = data frame storing parameters to be passed to the Query
# Return:
#   (1) a data frame storing result set OR
# e.g.
# Date: 2019-07-31 => #2019-07-31#
# POSIX: 2019-07-31 11:30:31 => #2019-07-31 11:30:31#
# character: ABCDE => 'ABCDE'
# numeric: 12345.123456789012345 => 12345.123456789012345
# NA: NA => NULL
################################################################################
qDBSQLite_sqlData <- function(dbconn, dfData)
{
    if (missing(dfData)) dfData <- NULL

    # nrow returns NULL if the data frame is NULL
    m <- nrow(dfData)
    n <- length(dfData)

    if (n==0) return (dfData)

    dbms_name <- dbGetInfo(dbconn)$dbms_name

    tmp_is_extendtypes <- tryCatch(
        dbconn@extend_types,
        error=function(e) {
            cat(sprintf("\t%s\n", e))
            return (FALSE)
        })

    tmp_lst <- dfData

    for (j in 1:n) {
        if (length(na.omit(grep(pattern="factor", class(tmp_lst[[j]])))) > 0) {
            # by default, it returns NULL for <NA>
            tmp_lst[[j]] <- as.character(tmp_lst[[j]])
        } # if

        if (length(na.omit(grep(pattern="character", class(tmp_lst[[j]])))) > 0) {
            # by default, it returns NULL for <NA>
            tmp_lst[[j]] <- dbQuoteString(dbconn, tmp_lst[[j]])
        } # if

        if (length(na.omit(grep(pattern="POSIX", class(tmp_lst[[j]])))) > 0)
        {
            if (dbms_name=="ACCESS") {
                # embraced with "#" for MS Access Only
                tmp_lst[[j]] <- format(tmp_lst[[j]], "#%Y-%m-%d %H:%M:%S#")
#            } else {
#                if (!(tmp_is_extendtypes)) {
#                    # treated as normal string for other DBMS
#                    tmp_lst[[j]] <- dbQuoteString(dbconn, format(tmp_lst[[j]], "%Y-%m-%d %H:%M:%OS"))
#                }
            }
        } # if

        if (length(na.omit(grep(pattern="Date", class(tmp_lst[[j]])))) > 0)
        {
            if (dbms_name=="ACCESS") {
                # embraced with "#" for MS Access Only
                tmp_lst[[j]] <- format(tmp_lst[[j]], "#%Y-%m-%d#")
#            } else {
#                if (!(tmp_is_extendtypes)) {
#                    # treated as normal string for other DBMS
#                    tmp_lst[[j]] <- dbQuoteString(dbconn, format(tmp_lst[[j]], "%Y-%m-%d"))
#                }
            }
        } # if

        if (data.class(tmp_lst[[j]])=="numeric") {
            tmp_lst[[j]] <- ifelse(is.na(tmp_lst[[j]]), NA, sprintf("%s", tmp_lst[[j]]))
        } # if

        tmp_lst[[j]] <- ifelse(is.na(tmp_lst[[j]]), "NULL", tmp_lst[[j]])
    } # for (j

    return (tmp_lst)
}

################################################################################
# Function: qDBSQLite_SendStatement
# Description:
#   To simplify the dbSendStatement process
# Parameters:
#	(1) dbconn = database connection
#   (2) chrSQL = SQL Command or parameterized Query denoted by ?
#   (3) dfParams = NULL or data frame storing parameters to be passed to the Query
# Return:
#   (1) 0 OR
#   (2) a data frame storing result set OR
#   (3) stop w/ error message after going through dfParams
# History:
# Date			Version	Author	Description
# 2019-08-09    ver001  SW
#
################################################################################
qDBSQLite_SendStatement <- function(dbconn, chrSQL, dfParams=NULL)
{
    # error count
    nErrors <- 0

    # return a data frame in case
    tmp_dfOut <- NULL

    #if (missing(dfParams)) dfParams <- NULL

    tmp_lst <- dfParams

    nFields <- length(dfParams)
    # nrow returns NULL if the data frame is NULL
    nRows <- nrow(dfParams)
    # assign nRows = 1 to force the call at least 1 time for no defined parameters
    nRows <- ifelse(is.null(dfParams), 1, nRows)

#   tmp_lst <- qDBSQLite_sqlData(dbconn, dfParams)
    tmp_lst <-  tryCatch(
        qDBSQLite_sqlData(dbconn, dfParams),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
            return (NULL)
        })

    # row by row
    for (i in 1:nRows) {
        # Construct SQL with parameters accordingly
        tmp_sql <- chrSQL

        if (nFields > 0) {
            for (j in 1:nFields) {
                tmp_sql <- sub(pattern="[?]", tmp_lst[i,j], tmp_sql)
            } # for (j
        } # if (nFields

        # debug (begin)
        #cat(sprintf("%s\n", tmp_sql))
        # debug (end)

        # only print the first 4 parameters
        cat(sprintf("\t\tParams = {%s, ...}\n", paste(tmp_lst[i,(1:min(4,length(tmp_lst)))], collapse=", ")))

        nError <- tryCatch(
                    {
                        rs <- dbSendStatement(dbconn, tmp_sql)
                        cat(sprintf("\t\t%.0f rows affected\n", dbGetRowsAffected(rs)))
                        tmp_df <- suppressWarnings(dbFetch(rs))
                        if (is.data.frame(tmp_df)&&(nrow(tmp_df) > 0)) tmp_dfOut <- rbind(tmp_dfOut, tmp_df)
                        dbClearResult(rs)
                    }, error=function(e) {
                        cat(sprintf("\t%s\n", e))
                        return (1)
                    })

        nErrors <- nErrors + nError

        tmp_curr <- i %% 100
        if ((i > 1) && ((tmp_curr < tmp_prev) || (i==nRows))) {
            cat(sprintf("\t\tProcessed %s of %s (%.2f pct).\n", i, nRows, 100*i/nRows))
        }

        tmp_prev <- tmp_curr
    } # for (i

    # only return a data frame for non-empty recordset
    if (is.data.frame(tmp_dfOut)&&(nrow(tmp_dfOut) > 0)) {
        return (tmp_dfOut)
    } # if

    if (nErrors > 0) {
        warnings(sprintf("%.0f error(s) when calling %s\n", nErrors, chrSQL))
    } # if

    return (0)
}
