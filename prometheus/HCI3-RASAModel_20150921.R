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

## Merge everything together
merge1 <- merge(wide_cost_use, wide_riskfactors, by = "epi_id", all = TRUE)
merge2 <- merge(merge1, wide_subtypes, by = "epi_id", all = TRUE)
final_wide_data <- merge(ra_episode_data, merge2, by = "epi_id", all.x = TRUE)
final_wide_data[is.na(final_wide_data)] <- 0
cat("Merges complete \n", file="Rrun.log", append=TRUE)

## Store final dataset
#write.csv(final_wide_data, "final_wide_data.csv", row.names = FALSE)
#final_wide_data <- read.csv("final_wide_data.csv")

## Read in the data dictionary
# data.dictionary <- read.csv("new_data_dictionary.csv")
data.dictionary <- filter(episode_ref, TYPE %in% c("Acute", "Other", "Chronic", "Procedural"))
cat("Data dictionary read successfully \n", file="Rrun.log", append=TRUE)

## Process all the episodes
epi.models <- lapply(data.dictionary$EPISODE_ID, function(epi) {

    epi.info <- data.dictionary[data.dictionary$EPISODE_ID == epi, ]

    # Print out the episode name to the console and get the info for it
    cat("\n", as.character(epi), " ", as.character(epi.info$NAME), "\n", file="Rrun.log", append=TRUE)

    # Try reading in the data
    per_sum <- try(filter(final_wide_data, epi_number == as.character(epi)))

    # Filter risk factors or subtypes present in 10 or fewer episodes
    rf_st_sums <- apply(per_sum[, grep("^RF|^ST", names(per_sum))], 2, sum)
    per_sum <- per_sum[,!(names(per_sum) %in% names(rf_st_sums[which(rf_st_sums <= 10)]))]

    # If successful, begin processing
    if (!inherits(per_sum, "try-error") && nrow(per_sum) > 0) {

        ###
        ### EOL Model
        ###
        # Get the list of risk factors / subtypes from the dataset
        risk_facs <- names(per_sum)[grep("^RF|^ST", names(per_sum))]
        if (length(risk_facs) == 0) risk_facs <- NULL
        final_risk_facs <- c("rec_enr", risk_facs)

        # Build the formula and fit the glm
        formula.str <- paste0("eol_ind ~ age + female + ", paste(final_risk_facs, collapse = " + "))
        eol.model <- glm(as.formula(formula.str), data = per_sum, family = "binomial")

        # Bind the eol probabilities to the data for this episode
        per_sum_eol <- cbind(per_sum, eol_prob = predict(eol.model, newdata = per_sum, type = "response"))

        # Compute the relevant levels and types for this episode
        epi.levels <- c("l1", ifelse(epi.info$TYPE %in% c("Chronic", "Other"), "l5", ifelse(epi.info$TYPE == "Procedural", "l3", "l4")))
        epi.types <- if (epi.info$TYPE %in% c("Chronic", "Other")) c("typ", "comp") else if (epi.info$TYPE %in% c("Procedural", "Acute")) c("typ_ip", "typ_other", "comp_other")
        epi.all <- paste(epi.types, rep(epi.levels, each = length(epi.types)), sep = "_") #cost_ra_typ_other_l4 missing
        epi.cost <- paste(c("ra", "sa"), rep(epi.all, each = 2), sep = "_")

        # Get the name of each column we need to fit models to
        colnames <- epi.cost[paste("cost", epi.cost, sep = "_") %in% names(per_sum_eol)]

        # Process each column one by one
        epi.results <- llply(colnames, function(choice) {

            # Get the use column and the cost column
            colname <- paste("use", sub("^ra_|^sa_", "", choice), sep = "_")
            colname_cost <- paste("cost", choice, sep = "_")

            ###
            ### Use Model
            ###
            # Calculate the percentage of 0s
            perc_0 <- sum(per_sum_eol[,colname_cost] == 0) / length(per_sum_eol[,colname_cost])
            use_prob <- rep(1, nrow(per_sum_eol))
            use.typ.model <- NULL

            # If the percentage exceeds 10%, we fit the model
            if (perc_0 >= .1) {

                # Get list of risk factors / subtypes again
                risk_facs <- names(per_sum_eol)[grep("^RF|^ST", names(per_sum_eol))]
                if (length(risk_facs) == 0) risk_facs <- NULL
                final_risk_facs <- c("eol_prob", risk_facs)

                # Fit the model and get the use probabilities
                formula.str <- paste0(colname, " ~ age + female + rec_enr + ", paste(final_risk_facs, collapse = " + "))
                use.typ.model <- glm(as.formula(formula.str), data = per_sum_eol, family = "binomial")
                use_prob <- predict(use.typ.model, newdata = per_sum_eol, type = "response")
            }

            ###
            ### Cost Model
            ###
            # Filter out only the episodes with non-zero cost
            per_sum_eol_filter <- per_sum_eol[per_sum_eol[,colname_cost] > 0, ]

            # Get the new list of risk factors and subtypes
            risk_facs <- names(per_sum_eol_filter)[grep("^RF|^ST", names(per_sum_eol_filter))]
            if (length(risk_facs) == 0) risk_facs <- NULL
            final_risk_facs <- c("eol_prob", risk_facs)

            # Fit the cost model
            formula.str <- paste0(colname_cost, " ~ age + female + rec_enr + ", paste(final_risk_facs, collapse = " + "))
            cost.model <- try(lm(as.formula(formula.str), data = per_sum_eol_filter), silent = TRUE)

            # Create vectors to store the results
            my.list <- list(rep(0, nrow(per_sum_eol)), rep(0, nrow(per_sum_eol)), rep(0, nrow(per_sum_eol)))
            names(my.list) <- c(paste0("use_prob_", choice), paste0("cost_pred_", choice), paste0("exp_cost_", choice))

            # Compute predictions and expected costs as long as the model did not fail
            if (!inherits(cost.model, "try-error")) {
                cost_pred <- predict(cost.model, newdata = per_sum_eol, type = "response")
                exp_cost <- use_prob * cost_pred

                # Build a list of the use probability, predicted cost, and expected cost
                my.list <- list(use_prob, cost_pred, exp_cost)
                names(my.list) <- c(paste0("use_prob_", choice), paste0("cost_pred_", choice), paste0("exp_cost_", choice))
            } else {
                cost.model <- NULL
            }

            # Return the coefficients as well as the expected costs
            return.list <- list(use = clean_coef(use.typ.model), cost = clean_coef(cost.model), expected = my.list)
            return.list
        })

        # Bind the epi id, eol probability, and the expected costs
        final.results <- cbind(epi_id = as.character(per_sum_eol$epi_id), eol_prob = per_sum_eol$eol_prob, as.data.frame(lapply(epi.results, function(elem){elem$expected})))

        # Compute the total expected cost for each level and ra/sa
        for (i in 1:length(epi.levels)) {
            for (j in c("ra", "sa")) {
                final.results[,paste0("total_exp_cost_", j, "_", epi.levels[i])] <- rowSums(as.data.frame(final.results[,grep(paste0("exp_cost_", j, "_.*_", epi.levels[i]), names(final.results))]))
            }
        }

        # Get each set of coefficients and name them appropriately
        my.coefs <- unlist(lapply(epi.results, function(elem){list(elem$use, elem$cost)}), recursive = FALSE)
        names(my.coefs) <- paste("coef", apply(expand.grid(c("use", "cost"), colnames), 1, function(row){paste(row[2], row[1], sep = "_")}), sep = "_")

        # Return the coefficients and the expected costs
        list(epi_number = epi.info$EPISODE_ID, epi_name = epi.info$NAME, coefs = c(list(eol = clean_coef(eol.model)), my.coefs), expected = final.results)
    }
})

cat("All episodes processed successfully \n", file="Rrun.log", append=TRUE)

## Name each element of the list according to the proper episode name
names(epi.models) <- data.dictionary$NAME

## Turn the list of expected costs into a single data frame
final_expected <- ldply(epi.models, function(model) { cbind(epi_number = model$epi_number, epi_name = model$epi_name, model$expected) })
final_expected[,1] <- NULL

## Turn the list of coefficients into a single data frame
final_coefs <- ldply(epi.models, function(model) {
    if (!is.null(model)) {

        ## Get the list of coefficients for this episode
        coef_list <- model$coefs
        nms <- unique(unlist(sapply(rev(coef_list), names)))

        ## Process the list of coefficients
        result <- sapply(coef_list, function(x) {

            ## If that coefficient shows up, store it, otherwise store NA
            my.vec <- NULL
            for (i in 1:length(nms)) {
                my.vec[i] <- if (nms[i] %in% names(x)) x[names(x) == nms[i]] else NA
            }
            names(my.vec) <- nms

            return(my.vec)
        })

        ## Return a data frame of the parameter names and their values
        cbind(epi_number = model$epi_number, epi_name = model$epi_name, parameter = nms, result)
    }
})
final_coefs[,1] <- NULL

##
## Final data is in final_expected and final_coefs
##

con <- dbConnect(MySQL(),
                 dbname=db,
                 user=dbuser,
                 password=dbpassword,
                 host=dbhost)

##
## Final data is in final_expected and final_coefs
##




##Write Coefficients and Expected Cost Files back to SQL database
dbWriteTable(conn=con,name="ra_exp_cost", value=final_expected,overwrite=TRUE)
dbWriteTable(conn=con,name="ra_coeffs", value=final_coefs,overwrite=TRUE)
#Optional- Write wide data file used to build models to SQL DB
dbWriteTable(conn=con,name="ra_final_data", value=final_wide_data,overwrite=TRUE)


cat("Completed HCI3 RASA Model \n", file="Rrun.log", append=TRUE)

###
###END
###
