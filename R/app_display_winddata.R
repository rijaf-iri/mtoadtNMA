#' Get wind data.
#'
#' Get wind data for wind barb display.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param tstep time step of the data.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartWindBarb <- function(net_aws, height, tstep, start, end, aws_dir)
{
    tz <- Sys.getenv("TZ")
    wnd <- getWindData(net_aws, height, tstep, start, end, aws_dir)
    if(wnd$status != "ok"){
        if(wnd$status == 'no-data')
            msg <- "No available data"
        if(wnd$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        ret <- list(status = msg)
        return(convJSON(ret))
    }

    time <- 1000 * as.numeric(as.POSIXct(wnd$date))
    dat <- do.call(cbind, wnd[c('ws', 'wd')])
    dat <- cbind(time, dat)
    dimnames(dat) <- NULL

    #########
    frmt <- if(tstep == "hourly") "%Y-%m-%d-%H" else "%Y-%m-%d-%H-%M"
    start <- strptime(start, frmt, tz = tz)
    end <- strptime(end, frmt, tz = tz)

    start <- format(start, "%Y-%m-%d %H:%M")
    end <- format(end, "%Y-%m-%d %H:%M")

    ttxt <- if(tstep == "hourly") "Hourly" else "15 minutes"
    stn <- paste(ttxt, "wind data at", height, "m:", wnd$id, "-", wnd$name)
    perd <- paste("Period:", start, "-", end)
    titre <- paste0(stn, "; ", perd)

    ret <- list(data = dat, title = titre, status = 'ok')
    return(convJSON(ret))
}

############
#' Get wind data.
#'
#' Get wind data to display wind rose.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param tstep time step of the data.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartWindRose <- function(net_aws, height, tstep, start, end, aws_dir)
{
    ret <- formatFreqTable(net_aws, height, tstep, start, end, aws_dir)
    return(convJSON(ret))
}


############
#' Plot wind rose.
#'
#' Plot wind rose with openair for download.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param tstep time step of the data.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return Png image
#' 
#' @export

openairWindRose <- function(net_aws, height, tstep, start, end, aws_dir)
{
    don <- getWindData(net_aws, height, tstep, start, end, aws_dir)
    if(don$status == "ok"){
        wind <- as.data.frame(don[c('date', 'ws', 'wd')])
        breaks <- c(0, 0.5, 2, 4, 6, 8, 12)
        labels <- c(" < 0.5", "0.5 to 2", "2 to 4", "4 to 6", "6 to 8", "8 to 12", " > 12")
        openair::windRose(wind, angle = 22.5, breaks = breaks,
                          auto.text= FALSE, paddle = FALSE,
                          key = list(labels = labels),
                          key.position = "right")
    }else{
        if(don$status == 'no-data')
            msg <- "No available data"
        if(don$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        plot(0:1, 0:1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '')
        text(0.5, 0.5, msg)
    }
}

############
#' Plot wind contour.
#'
#' Plot wind contour.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param tstep time step of the data.
#' @param start start time.
#' @param end end time.
#' @param centre center of the image, N, W, S, E.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return Png image
#' 
#' @export

graphWindContours <- function(net_aws, height, tstep,
                              start, end, centre, aws_dir)
{
    don <- getWindData(net_aws, height, tstep, start, end, aws_dir)

    if(don$status != "ok"){
        if(don$status == 'no-data')
            msg <- "No available data"
        if(don$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
        text(1, 1, msg, font = 2, cex = 1.5)
    }else{
        hr <- format(don$date, "%H")

        ttxt <- if(tstep == "hourly") "Hourly" else "15 Minutes"
        titre <- paste(ttxt, "wind data:", don$name, "-", don$id, "-", don$network)
        subtitre <- paste("Period:", min(don$date), "-", max(don$date),
                          "; Data availability:", paste0(don$avail, "%"))

        windContours(hour = hr,
                     wd = don$wd,
                     ws = don$ws,
                     centre = centre,
                     ncuts = 0.5,
                     spacing = 2,
                     key.spacing = 2,
                     smooth.contours = 1.5,
                     smooth.fill = 1.5,
                     title = list(title = titre, subtitle = subtitre)
                   )
    }
}

###########################

formatFreqTable <- function(net_aws, height, tstep, start, end, aws_dir)
{
    don <- windFrequencyTable(net_aws, height, tstep, start, end, aws_dir)
    if(don$status != 'ok') return(don)

    wind <- don$freq
    cnom <- colnames(wind)
    rnom <- rownames(wind)
    scol <- colSums(wind)
    srow <- rowSums(wind)
    wind <- cbind(wind, srow)
    wind <- rbind(wind, c(scol, NA))
    wind <- rbind(wind, c(don$mean, rep(NA, ncol(wind) - 1)))
    wind <- rbind(wind, c(don$calm, rep(NA, ncol(wind) - 1)))
    wind <- rbind(wind, c(don$avail, rep(NA, ncol(wind) - 1)))

    dimnames(wind) <- NULL
    wind <- round(wind, 2)
    wind[18, 1] <- paste(wind[18, 1], "m/s")
    wind[19, 1] <- paste(wind[19, 1], "%")
    wind[20, 1] <- paste(wind[20, 1], "%")
    wind <- cbind(c(rnom, "Total", "Mean Speed", "Percent Calm", "Data Availability"), wind)
    wind[is.na(wind)] <- ''
    wind <- as.data.frame(wind, stringsAsFactors = FALSE)
    names(wind) <- c('Direction', paste(cnom, 'm/s'), 'Total')

    don$freq <- as.list(wind)

    return(don)
}

###########################

windFrequencyTable <- function(net_aws, height, tstep, start, end, aws_dir)
{
    don <- getWindData(net_aws, height, tstep, start, end, aws_dir)
    if(don$status != "ok"){
        if(don$status == 'no-data')
            msg <- "No available data"
        if(don$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        ret <- list(status = msg)
        return(ret)
    }

    #####
    ina <- !is.na(don$ws) & !is.na(don$wd)
    nobs <- sum(ina)
    if(nobs == 0){
        ret <- list(status = "No available data")
        return(ret)
    }

    don$date <- don$date[ina]
    don$ws <- don$ws[ina]
    don$wd <- don$wd[ina]

    #####
    icalm <- don$ws == 0 | don$wd == 0
    mean.s <- mean(don$ws)
    wcalm <- lapply(don[c('date', 'ws', 'wd')], function(v) v[icalm])
    ncalm <- length(wcalm$date)
    freq.calm <- if(ncalm == 0) 0 else 100 * ncalm/nobs

    don$date <- don$date[!icalm]
    don$ws <- don$ws[!icalm]
    don$wd <- don$wd[!icalm]
    nobs <- length(don$date)

    s.breaks <- c(0, 0.5, 2, 4, 6, 8, 12, 100)
    s.labels <- c(" < 0.5", "0.5 to 2", "2 to 4", "4 to 6", "6 to 8", "8 to 12", " > 12")

    dres <- 22.5
    d.breaks <- c(0, seq(dres/2, 360 - dres/2, by = dres), 360)
    d.labels <- c("N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW", "N")

    s.binned <- cut(x = don$ws, breaks = s.breaks, labels = s.labels, ordered_result = TRUE)
    d.binned <- cut(x = don$wd, breaks = d.breaks, ordered_result = TRUE)
    levels(d.binned) <- d.labels

    freq <- table(d.binned, s.binned)
    freq <- 100 * freq/nobs

    start <- min(don$date)
    end <- max(don$date)

    list(id = don$id, name = don$name, network = don$network, height = height,
         calm = freq.calm, mean = mean.s, freq = freq, avail = don$avail,
         timestep = tstep, start = start, end = end, status = 'ok')
}

###########################

## adapted the function windContours from https://github.com/tim-salabim/metvurst/
windContours <- function(hour, wd, ws, centre = "S",
                         ncuts = 0.5, spacing = 2, key.spacing = 2,
                         smooth.contours = 1.2, smooth.fill = 1.2,
                         colour = rev(RColorBrewer::brewer.pal(11, "Spectral")),
                         gapcolor = "grey50",
                         title = NULL)
{
    centre <- if(centre == "E") "ES" else centre
    levs <- switch(centre,
                   "N" = c(19:36, 1:18),
                   "ES" = c(28:36, 1:27),
                   "S" = 1:36,
                   "W" = c(10:36, 1:9))

    wdn <- c(45, 90, 135, 180, 225, 270, 315, 360)
    wdc <- c("NE", "E", "SE", "S", "SW", "W", "NW", "N")
    label <- switch(centre,
                    "N" = c(225, 270, 315, 360, 45, 90, 135, 180),
                    "ES" = c(315, 360, 45, 90, 135, 180, 225, 270),
                    "S" = c(45, 90, 135, 180, 225, 270, 315, 360),
                    "W" = c(135, 180, 225, 270, 315, 360, 45, 90))
    label <- wdc[match(label, wdn)]

    hour <- as.numeric(hour)
    dircat <- ordered(ceiling(wd/10), levels = levs, labels = 1:36)
    tab.wd <- stats::xtabs(~ dircat + hour)

    tab.wd_smooth <- fields::image.smooth(tab.wd, theta = smooth.contours, xwidth = 0, ywidth = 0)
    freq.wd <- matrix(prop.table(tab.wd_smooth$z, 2)[, 24:1] * 100, nrow = 36, ncol = 24)

    tab.add_smooth <- fields::image.smooth(tab.wd, theta = smooth.fill, xwidth = 0, ywidth = 0)
    mat.add <- matrix(prop.table(tab.add_smooth$z, 2)[, 24:1] * 100,  nrow = 36, ncol = 24)

    zlevs.fill <- seq(floor(min(mat.add)), ceiling(max(mat.add)), by = ncuts)
    zlevs.conts <- seq(floor(min(freq.wd)), ceiling(max(freq.wd)), by = spacing)

    kolorfun <- grDevices::colorRampPalette(colour)
    kolor <- kolorfun(length(zlevs.fill) - 1)

    mat.add <- rbind(mat.add, mat.add[1, ])
    freq.wd <- rbind(freq.wd, freq.wd[1, ])
    xax <- 1:37
    yax <- 0:23

    ####

    layout.matrix <- matrix(c(0, 1, 1, 1, 1, 0,
                              2, 2, 2, 3, 3, 3,
                              0, 0, 4, 4, 0, 0,
                              0, 0, 5, 5, 0, 0),
                              ncol = 6, nrow = 4, byrow = TRUE)
    layout(layout.matrix, widths = 1, heights = c(0.11, 0.9, 0.07, 0.07), respect = FALSE)

    ####

    op <- par(mar = c(0.1, 1, 1, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1.1, title$title, font = 2, cex = 2)
    text(1, 0.7, title$subtitle, font = 1, cex = 1.6)
    par(op)

    #### 
    op <- par(mar = c(5.1, 5.1, 0, 0))
    plot(1, xlim = range(xax), ylim = c(-0.5, 23.5), xlab = "", ylab = "",
         type = "n", xaxt = 'n', yaxt = 'n', xaxs = "i", yaxs = "i")
    axis(side = 1, at = seq(4.5, 36 ,by = 4.5), labels = label, font = 2, cex.axis = 1.2)
    axis(side = 2, at = seq(22, 2, -2) - 1, labels = seq(2, 22, 2), las = 1, font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Direction", font = 2, cex = 0.9)
    mtext(side = 2, line = 3, "Hour", font = 2, cex = 0.9)
    box()

    .filled.contour(xax, yax, mat.add, levels = zlevs.fill, col = kolor)
    .filled.contour(xax, yax, mat.add, levels = seq(0, 0.2, 0.1), col = gapcolor)
    contour(xax, yax, freq.wd, add = TRUE, levels = zlevs.conts, col = "grey10", labcex = 0.7,
            labels = seq(zlevs.fill[1], zlevs.fill[length(zlevs.fill)], key.spacing))
    contour(xax, yax, freq.wd, add = TRUE, levels = 0.5, col = "grey10", lty = 3, labcex = 0.7)
    par(op)

    ####
    op <- par(mar = c(5.1, 0, 0, 1.2))
    plot(1, xlim = range(ws, na.rm = TRUE) + c(-0.1, 0.5), ylim = c(0.5, 24.5), type = 'n',
         xaxt = 'n', yaxt = 'n', xlab = "", ylab = "", xaxs = "i", yaxs = "i")
    abline(h = seq(22, 2, -2), col = "lightgray", lty = "dotted", lwd = 1.0)
    abline(v = axTicks(1), col = "lightgray", lty = "solid", lwd = 0.9)
    axis(side = 1, at = axTicks(1), font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Speed [m/s]", font = 2, cex = 0.9)

    boxplot(ws ~ rev(hour), horizontal = TRUE, xaxt = 'n', yaxt = 'n', add = TRUE, notch = TRUE,
            col = 'lightblue', medcol = 'red', whiskcol = 'blue', staplecol = 'blue',
            boxcol = 'blue', outcol = 'blue', outbg = 'lightblue', outcex = 0.7, outpch = 21, boxwex = 0.4)
    par(op)

    ####
    op <- par(mar = c(2.0, 1, 0, 1))
    nBreaks<- length(zlevs.fill)
    midpoints<- (zlevs.fill[1:(nBreaks - 1)] +  zlevs.fill[2:nBreaks])/2
    mat.lez <- matrix(midpoints, nrow = 1, ncol = length(midpoints)) 
    image(zlevs.fill, 1:2, t(mat.lez), col = kolor, breaks = zlevs.fill, xaxt = "n", yaxt = "n", xlab = "", ylab = "")
    axis.args <- list(side = 1, mgp = c(3, 1, 0), las = 0, font = 2, cex.axis = 1.5)
    do.call("axis", axis.args)
    box()
    par(op)

    op <- par(mar = c(0, 1, 0, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1, "Frequencies (in %)", font = 2, cex = 1.5)
    par(op)

    invisible()
}

