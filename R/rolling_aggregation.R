
.rollfun.vec <- function(x, win, fun, na.rm, min.data, na.pad, fill, align)
{
    conv <- if(fun == "convolve") TRUE else FALSE
    fun <- match.fun(fun)
    nl <- length(x)
    xna <- xx <- rep(NA, nl - win + 1)
    for(k in seq(nl - win + 1)){
        if(conv){
            vx <- x[seq(k, k + win - 1, 1)]
            vx <- vx[!is.na(vx)]
            ix <- length(vx)
            xx[k] <- if(ix > 1) stats::convolve(vx, rep(1/ix, ix), type = "filter") else NA
        }else xx[k] <- fun(x[seq(k, k + win - 1, 1)], na.rm = na.rm)
        xna[k] <- sum(!is.na(x[seq(k, k + win - 1, 1)]))
    }
    xx[is.nan(xx) | is.infinite(xx)] <- NA
    xx[xna < min.data] <- NA

    if(na.pad){
        if(align == "right"){
            xx <-
                if(fill)
                    c(rep(xx[1], win - 1), xx)
                else
                    c(rep(NA, win - 1), xx)
        }
        if(align == "left"){
            xx <-
                if(fill)
                    c(xx, rep(xx[length(xx)], win - 1))
                else
                    c(xx, rep(NA, win - 1))
        }
        if(align == "center"){
            before <- floor((win - 1) / 2)
            after <- ceiling((win - 1) / 2)
            xx <- 
                if(fill) 
                    c(rep(xx[1], before), xx, rep(xx[length(xx)], after))
                else
                    c(rep(NA, before), xx, rep(NA, after))
        }
    }
    return(xx)
}

.rollfun.mat <- function(x, win, fun, na.rm, min.data, na.pad, fill, align)
{
    nl <- nrow(x)
    nc <- ncol(x)
    xna <- xx <- matrix(NA, nrow = nl - win + 1, ncol = nc)

    foo <- switch(fun,
                  "sum" = colSums,
                  "mean" = colMeans,
                  "median" = matrixStats::colMedians,
                  "max" = matrixStats::colMaxs,
                  "min" = matrixStats::colMins,
                  "var" = matrixStats::colVars,
                  "sd" = matrixStats::colSds
                )

    for(k in seq(nl - win + 1)){
        xx[k, ] <- foo(x[seq(k, k + win - 1, 1), , drop = FALSE], na.rm = na.rm)
        xna[k, ] <- colSums(!is.na(x[seq(k, k + win - 1, 1), , drop = FALSE]))
    }
    xx[is.nan(xx) | is.infinite(xx)] <- NA
    xx[xna < min.data] <- NA

    if(na.pad){
        if(align == "right"){
            xx <- 
                if(fill)
                    rbind(matrix(xx[1, ], win - 1, nc, byrow = TRUE), xx)
                else
                    rbind(matrix(NA, win - 1, nc), xx)
        }
        if(align == "left"){
            xx <-
                if(fill)
                    rbind(xx, matrix(xx[nrow(xx), ], win - 1, nc, byrow = TRUE))
                else
                    rbind(xx, matrix(NA, win - 1, nc))
        }
        if(align == "center"){
            before <- floor((win - 1) / 2)
            after <- ceiling((win - 1) / 2)
            xx <-
                if(fill)
                    rbind(matrix(xx[1, ], before, nc, byrow = TRUE), xx, matrix(xx[nrow(xx), ], after, nc, byrow = TRUE))
                else
                    rbind(matrix(NA, before, nc), xx, matrix(NA, after, nc))
        }
    }
    return(xx)
}

## to export
cdt.roll.fun <- function(x, win, fun = "sum", na.rm = FALSE,
                        min.data = win, na.pad = TRUE, fill = FALSE,
                        align = c("center", "left", "right")
                    )
{
    # vector, fun: sum, mean, median, var, sd, max, min, convolve
    # matrix, fun: sum, mean, median, var, sd, max, min
    if(is.matrix(x)) foo <- .rollfun.mat
    else if(is.vector(x)) foo <- .rollfun.vec
    else return(NULL)

    align <- match.arg(align)
    if(min.data > win) min.data <- win

    foo(x, win, fun, na.rm, min.data, na.pad, fill, align)
}
