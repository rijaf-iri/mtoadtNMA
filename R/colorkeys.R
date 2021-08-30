
image.plot_Legend_pars <- function(Zmat, userOp){
    brks0 <- pretty(Zmat, n = 10, min.n = 5)
    brks0 <- if(length(brks0) > 0) brks0 else c(0, 1)
    breaks <- if(userOp$levels$custom) userOp$levels$levels else brks0
    breaks[length(breaks)] <- breaks[length(breaks)] + 1e-15

    ## legend label breaks
    legend.label <- breaks

    breaks1 <- if(userOp$levels$equidist) seq(0, 1, length.out = length(breaks)) else breaks

    Zmat <- Zmat + 1e-15
    Zrange <- if(all(is.na(Zmat))) c(0, 1) else range(Zmat, na.rm = TRUE)

    brks0 <- range(brks0)
    brks1 <- range(breaks)
    brn0 <- min(brks0[1], brks1[1])
    brn1 <- max(brks0[2], brks1[2])
    if(brn0 == breaks[1]) brn0 <- brn0 - 1
    if(brn1 == breaks[length(breaks)]) brn1 <- brn1 + 1
    if(brn0 > Zrange[1]) brn0 <- Zrange[1]
    if(brn1 < Zrange[2]) brn1 <- Zrange[2]
    breaks <- c(brn0, breaks, brn1)
    tbrks2 <- breaks1[c(1, length(breaks1))] + diff(range(breaks1)) * 0.02 * c(-1, 1)
    breaks2 <- c(tbrks2[1], breaks1, tbrks2[2])
    zlim <- range(breaks2)

    ## colors
    if(userOp$uColors$custom){
        kolFonction <- grDevices::colorRampPalette(userOp$uColors$color)
        kolor <- kolFonction(length(breaks) - 1)
    }else{
        kolFonction <- get(userOp$pColors$color, mode = "function")
        kolor <- kolFonction(length(breaks) - 1)
        if(userOp$pColors$reverse) kolor <- rev(kolor)
    }
    
    ## bin
    ## < x1; [x1, x2[; [x2, x3[; ...; [xn-1, xn]; > xn

    list(breaks = breaks,
         legend.breaks = list(zlim = zlim, breaks = breaks2),
         legend.axis = list(at = breaks1, labels = legend.label),
         colors = kolor)
}

#####

defColorKeyOptions <- function(var.name, aws = TRUE, timestep = "minute",
                               colorP = "tim.colors", reverse = FALSE,
                               customC = FALSE, colorC = NULL,
                               customL = FALSE, levels = NULL, equidist = FALSE
                             )
{
    lvl <- NULL
    if(aws){
        rrlev <- switch(timestep,
                        "minute" = c(0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 50),
                        "hourly" = c(0, 1, 2, 5, 10, 15, 20, 30, 50, 70, 100),
                        "daily" = c(0, 1, 3, 5, 10, 20, 30, 50, 70, 100, 130),
                            c(0, 5, 10, 20, 30, 50, 70, 100, 150, 200, 300, 500)
                       )

        lvl <- switch(var.name,
                      "RR" = rrlev,
                      "TT" = seq(-4, 36, 4),
                      "TD" = seq(-12, 30, 4),
                      "RH" = seq(45, 100, 5),
                      "PR" = seq(750, 1025, 25),
                      "MSL" = seq(850, 1100, 25),
                      "RG" = seq(0, 1100, 100),
                      "FF" = c(0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15),
                      "SM" = seq(0, 100, 10),
                      "ST" = seq(-12, 30, 4),
                      "LWT" = seq(0, 10, 1)
                     )
        customL <- TRUE
        equidist <- TRUE
    }

    op <- list(pColors = list(color = colorP, reverse = reverse),
               uColors = list(custom = customC, color = colorC),
               levels = list(custom = customL, levels = lvl, equidist = equidist)
              )

    image.plot_Legend_pars(levels, op)
}

#####

defColorKeyOptionsAcc <- function(timestep = "minute",
                                  colorP = "tim.colors", reverse = FALSE,
                                  customC = FALSE, colorC = NULL,
                                  customL = FALSE, levels = NULL, equidist = FALSE
                                 )
{
    lvl <- NULL
    lvl <- switch(timestep,
                  "hourly" = c(0, 5, 10, 20, 30, 50, 70, 100, 150, 200, 300, 500),
                  "daily" = c(0, 10, 30, 50, 70, 100, 150, 200, 300, 500, 700)
                 )
    customL <- TRUE
    equidist <- TRUE

    op <- list(pColors = list(color = colorP, reverse = reverse),
               uColors = list(custom = customC, color = colorC),
               levels = list(custom = customL, levels = lvl, equidist = equidist)
             )

    image.plot_Legend_pars(levels, op)
}

