
#' Read AWS metadata.
#'
#' Read AWS coordinates and parameters.
#' 
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readCoords <- function(aws_dir){
    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    return(convJSON(awsPars))
}

#############
#' Get AWS coordinates.
#'
#' Get AWS coordinates to display on map.
#' 
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readCoordsMap <- function(aws_dir){
    on.exit(DBI::dbDisconnect(con_adt))
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    con_adt <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(con_adt, "try-error")){
        return(convJSON(NULL))
    }

    adcoCrd <- DBI::dbReadTable(con_adt, "adcon_crds")
    adcoCrd$network <- "Adcon"
    vaisCrd <- DBI::dbReadTable(con_adt, "vaisala_crds")
    vaisCrd$network <- "Vaisala"
    koikCrd <- DBI::dbReadTable(con_adt, "koica_crds")
    koikCrd$network <- "Koica"

    nmCol <- c("id", "name", "longitude", "latitude", "altitude", "network",
                "woreda", "zone", "region", "startdate", "enddate")

    # crds <- rbind(adcoCrd[, nmCol, drop = FALSE], vaisCrd[, nmCol, drop = FALSE])
    crds <- rbind(adcoCrd[, nmCol, drop = FALSE],
                  vaisCrd[, nmCol, drop = FALSE],
                  koikCrd[, nmCol, drop = FALSE])

    #############
    xcrd <- crds[, c('longitude', 'latitude')]
    xcrd <- paste(xcrd[, 1], xcrd[, 2], sep = "_")
    ix1 <- duplicated(xcrd) & !is.na(crds$longitude)
    ix2 <- duplicated(xcrd, fromLast = TRUE) & !is.na(crds$longitude)
    ix <- ix1 | ix2
    icrd <- unique(xcrd[ix])

    #############

    crds <- apply(crds, 2, as.character)
    crds <- cbind(crds, StatusX = "blue")

    #############

    crds[crds[, "network"] == "Vaisala", "StatusX"] <- "blue"
    crds[crds[, "network"] == "Adcon", "StatusX"] <- "green"
    crds[crds[, "network"] == "Koica", "StatusX"] <- "violet"

    #############
    if(length(icrd) > 0){
        for(jj in icrd){
            ic <- xcrd == jj
            xx <- apply(crds[ic, ], 2, paste0, collapse = " | ")
            xx <- matrix(xx, nrow = 1, dimnames = list(NULL, names(xx)))
            xx <- do.call(rbind, lapply(seq_along(which(ic)), function(i) xx))

            xcr <- crds[ic, c('longitude', 'latitude')]
            crds[ic, ] <- xx
            crds[ic, c('longitude', 'latitude')] <- xcr
            crds[ic, 'StatusX'] <- "red"
        }
    }

    #############
    crds[is.na(crds)] <- ""
    crds <- cbind(crds, LonX = crds[, 3], LatX = crds[, 4])
    ix <- crds[, 'LonX'] == "" | crds[, 'LatX'] == ""
    crds[ix, c('LonX', 'LatX')] <- NA
    crds <- as.data.frame(crds)
    crds$LonX <- as.numeric(as.character(crds$LonX))
    crds$LatX <- as.numeric(as.character(crds$LatX))

    #############
    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    #############
    # get parameters for each aws
    # crds$PARS <- pars

    return(convJSON(crds))
}

#############
#' Get AWS start and end time.
#'
#' Get the start and end time of a specified AWS.
#' 
#' @param id ID of the AWS.
#' @param network the AWS network code; 1: vaisala, 2: adcon, 3: koika.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

getAWSTimeRange <- function(id, network, aws_dir){
    on.exit(DBI::dbDisconnect(con_adt))
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    con_adt <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(con_adt, "try-error")){
        return(convJSON(NULL))
    }

    net_dat <- switch(as.character(network),
                      "1" = "vaisala_crds",
                      "2" = "adcon_crds",
                      "3" = "koica_crds"
                    )

    query <- paste0("SELECT startdate, enddate FROM ", net_dat, " WHERE id='", id, "'")
    # qres <- getQuery(con_adt, query)
    qres <- DBI::dbGetQuery(con_adt, query)
    qres <- lapply(qres, as.POSIXct, origin = origin, tz = tz)
    qres <- lapply(qres, format, "%Y-%m-%d %H:%M:%S")

    return(convJSON(qres))
}

