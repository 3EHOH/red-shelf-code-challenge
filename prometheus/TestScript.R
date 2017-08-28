###
### HCI Data Transformations and Modeling Routine
###

## Set DB Name
##db<-"Test_TriggerFix20150911"
##dbuser<-"xxxx"
##dbpassword<-"xxxxx"
##dbhost<-"xxxx"

## Load required packages
library(plyr)
library(dplyr)
library(tidyr)
library(RMySQL)
library(ggplot2)

## Function to return NA if the model computed was NULL
clean_coef <- function(model) {
     if (is.null(model)) NA else coef(model) 
}

## Function to convert one of the long datasets into wide format
long_to_wide <- function(data) {

    ## Long to wide routine
    wide_data <- data %>% spread(name, value)

    ## Set NAs and negative values to 0
    wide_data[is.na(wide_data)] <- 0
    wide_data[,-1][wide_data[,-1] < 0] <- 0

    return(wide_data)
}

cat("Starting HCI3 RASA Model \n", file="Rrun.log", append=TRUE)

## Connect to the database and read in the data
con <- dbConnect(MySQL(),
                 dbname=db,
                 user=dbuser,
                 password=dbpassword,
                 host=dbhost)

ra_episode_data <- dbReadTable(con, "ra_episode_data")
ra_cost_use <- dbReadTable(con, "ra_cost_use")
ra_subtypes <- dbReadTable(con, "ra_subtypes")
ra_riskfactors <- dbReadTable(con, "ra_riskfactors")
episode_ref <- dbReadTable(con,"build_episode_reference")
cat("Reference tables successfully read \n", file="Rrun.log", append=TRUE)

## Remove epi number column (we only need this in the episode data)
ra_cost_use$epi_number <- NULL
ra_subtypes$epi_number <- NULL
ra_riskfactors$epi_number <- NULL

## Process the data from long to wide
wide_subtypes <- long_to_wide(ra_subtypes)
wide_riskfactors <- long_to_wide(ra_riskfactors)
wide_cost_use <- long_to_wide(ra_cost_use)
cat("Long to wide processing done \n", file="Rrun.log", append=TRUE)
