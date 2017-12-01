###
### HCI Data Transformations and Modeling Routine
###

##
## Configuration
##
per_episode <- TRUE
dbtype <- "RMySQL" # one of vRODBC, RMySQL, or RJDBC
use_lasso <- FALSE
use_car <- TRUE
use_alt_association <- TRUE
output <- db # one of none, db, csv

## Select a Database
mydb <- db
host <- dbhost # or "jdbc:vertica://localhost:5433/hci3" or "VerticaSQL"

##
## Libraries
##
library(plyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(dbtype, character.only = TRUE)
if (use_car) library(car)

##
## Utility Functions
##

## Function to fit a LASSO model
LASSO_Regression <- function(data, y, x, ...) {
    data.subset <- data[,c(x, y)]
    data.complete <- data.subset[complete.cases(data.subset),]
    
    las.result<-cv.glmnet(data.matrix(data.complete[,x]), data.complete[,y], ...)
    
    las.pred <- predict(las.result, newx = data.matrix(data.complete[,x]), type = "response", s = las.result$lambda.min)
    
    return(list(name = "las", model = las.result, predictions = las.pred, auc = auc(data.complete[,y], las.pred), x = x))
}

## Function to return NA if the model computed was NULL
clean_coef <- function(model) {
    if (is.null(model)) NA else coef(model)
}

## Function to convert one of the long datasets into wide format
long_to_wide <- function(data) {
    if (nrow(data) == 0) return(data)
    
    ## Long to wide routine
    wide_data <- data %>% filter(!duplicated(.[,-3])) %>% spread(name, value)
    
    ## Set NAs and negative values to 0
    wide_data[is.na(wide_data)] <- 0
    wide_data[,-1][wide_data[,-1] < 0] <- 0
    
    return(wide_data)
}

##
## Database Connection
##
if (dbtype == "vRODBC") {
    con <- odbcConnect(host, uid = "epbuilder", pwd = "dev420!Xw")
    myfunc <- sqlQuery
} else if (dbtype == "RMySQL") {
    con <- dbConnect(MySQL(), dbname = mydb, user = dbuser, password = dbpassword, host = host)
    myfunc <- dbGetQuery
    mydb <- ""
} else if (dbtype == "RJDBC") {
    drv <- JDBC("com.vertica.jdbc.Driver",
                "/opt/vertica/java/lib/vertica-jdbc-7.2.1-0.jar",
                identifier.quote="`")
    con <- dbConnect(drv, host, "epbuilder", "dev420!Xw")
    myfunc <- dbGetQuery
}

episode_ref <- as.character(myfunc(con, paste0("SELECT DISTINCT epi_number FROM ", paste(mydb, "ra_episode_data", sep = ".")))[,1])
episode_typechar <- sapply(strsplit(episode_ref, ""), `[[`, 2)
episode_typelist <- c("A" = "Acute", "C" = "Chronic", "P" = "Procedural", 
                      "S" = "System Related Failure", "X" = "Other")
episode_type <- episode_typelist[episode_typechar]

data.dictionary <- data.frame(EPISODE_ID = episode_ref, TYPE = episode_type) %>%
    filter(TYPE %in% c("Acute", "Chronic", "Procedural", "Other")) %>%
    mutate(EPISODE_ID = as.character(EPISODE_ID), TYPE = as.character(TYPE)) %>%
    arrange(EPISODE_ID)

if (!per_episode) {
    ra_episode_data <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_episode_data", sep = ".")))
    ra_cost_use <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_cost_use", sep = ".")))
    ra_subtypes <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_subtypes", sep = ".")))
    ra_riskfactors <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_riskfactors", sep = ".")))
    
    ## Remove epi number column (we only need this in the episode data)
    ra_cost_use$epi_number <- NULL
    ra_subtypes$epi_number <- NULL
    ra_riskfactors$epi_number <- NULL
    
    ##########
    ##Fix to convert character variables to numeric
    ##########
    ra_episode_data$female<-as.integer(ra_episode_data$female)
    ra_episode_data$age<-as.integer(ra_episode_data$age)
    ra_episode_data$rec_enr<-as.integer(ra_episode_data$rec_enr)
    ra_episode_data$eol_ind<-as.integer(ra_episode_data$eol_ind)
    ra_riskfactors$value<-as.integer(ra_riskfactors$value)
    ra_subtypes$value<-as.integer(ra_subtypes$value)
    ra_cost_use$value<-as.integer(ra_cost_use$value)
    
    ## Process the data from long to wide
    wide_subtypes <- long_to_wide(ra_subtypes)
    wide_riskfactors <- long_to_wide(ra_riskfactors)
    wide_cost_use <- long_to_wide(ra_cost_use)
    
    rm(ra_subtypes)
    rm(ra_riskfactors)
    rm(ra_cost_use)
    gc()
    
    ## Merge everything together
    merge1 <- left_join(wide_cost_use, wide_riskfactors, by = c("epi_id" = "epi_id"))
    merge2 <- left_join(merge1, wide_subtypes, by = c("epi_id" = "epi_id"))
    final_wide_data <- left_join(ra_episode_data, merge2, by = c("epi_id" = "epi_id"))
    final_wide_data[is.na(final_wide_data)] <- 0
    
    rm(merge1)
    rm(merge2)
    gc()
    
    rm(wide_subtypes)
    rm(wide_riskfactors)
    rm(wide_cost_use)
}

##
## Primary Model Fitting Routine
##
epi.models <- lapply(data.dictionary$EPISODE_ID, function(epi) {
    
    # Print out the episode name to the console and get the info for it
    cat("\n", epi, "\n")
    
    epi.info <- data.dictionary[data.dictionary$EPISODE_ID == epi, ]
    
    if (per_episode) {
        ra_episode_data <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_episode_data", sep = "."), " WHERE epi_number = '", epi, "'"))
        ra_cost_use <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_cost_use", sep = "."), " WHERE epi_number = '", epi, "'"))
        ra_subtypes <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_subtypes", sep = "."), " WHERE epi_number = '", epi, "'"))
        ra_riskfactors <- myfunc(con, paste0("SELECT * FROM ", paste(mydb, "ra_riskfactors", sep = "."), " WHERE epi_number = '", epi, "'"))
        
        ## Remove epi number column (we only need this in the episode data)
        ra_cost_use$epi_number <- NULL
        ra_subtypes$epi_number <- NULL
        ra_riskfactors$epi_number <- NULL
        
        ##########
        ##Fix to convert character variables to numeric
        ##########
        ra_episode_data$female<-as.integer(ra_episode_data$female)
        ra_episode_data$age<-as.integer(ra_episode_data$age)
        ra_episode_data$rec_enr<-as.integer(ra_episode_data$rec_enr)
        ra_episode_data$eol_ind<-as.integer(ra_episode_data$eol_ind)
        ra_riskfactors$value<-as.integer(ra_riskfactors$value)
        ra_subtypes$value<-as.integer(ra_subtypes$value)
        ra_cost_use$value<-as.integer(ra_cost_use$value)
        
        ## Process the data from long to wide
        wide_subtypes <- long_to_wide(ra_subtypes)
        wide_riskfactors <- long_to_wide(ra_riskfactors)
        wide_cost_use <- long_to_wide(ra_cost_use)
        
        rm(ra_subtypes)
        rm(ra_riskfactors)
        rm(ra_cost_use)
        gc()
        
        ## Merge everything together
        merge1 <- left_join(wide_cost_use, wide_riskfactors, by = c("epi_id" = "epi_id"))
        merge2 <- left_join(merge1, wide_subtypes, by = c("epi_id" = "epi_id"))
        final_wide_data <- left_join(ra_episode_data, merge2, by = c("epi_id" = "epi_id"))
        final_wide_data[is.na(final_wide_data)] <- 0
        
        rm(merge1)
        rm(merge2)
        gc()
        
        # Try reading in the data
        per_sum <- final_wide_data
        
        rm(wide_subtypes)
        rm(wide_riskfactors)
        rm(wide_cost_use)
    } else {
        per_sum <- filter(final_wide_data, epi_number == epi)
    }
    
    # Filter risk factors or subtypes present in 10 or fewer episodes
    rf_st_sums <- apply(per_sum[, grep("^RF|^ST", names(per_sum))], 2, function(x) { sum(as.numeric(x)) })
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
        if (length(colnames) == 0) return(NULL)
        
        # Process each column one by one
        epi.results <- llply(colnames, function(choice) {
          
            if (use_alt_association) {
                mylvl_list <- strsplit(choice, "_")
                mylvl <- mylvl_list[[1]][length(mylvl_list[[1]])]
                if (mylvl %in% c("l3", "l4", "l5") && "associated" %in% names(per_sum_eol)) {
                    per_sum_eol <- filter(per_sum_eol, associated == 0)
                }    
            }
            
            # Get the use column and the cost column
            colname <- paste("use", sub("^ra_|^sa_", "", choice), sep = "_")
            colname_cost <- paste("cost", choice, sep = "_")
            
            ###
            ### Use Model
            ###
            # Calculate the percentage of 0s
            perc_0 <- sum(as.numeric(per_sum_eol[,colname_cost] == 0)) / length(per_sum_eol[,colname_cost])
            use_prob <- rep(1, nrow(per_sum_eol))
            use.model.diag <- NULL
            use.typ.model <- NULL
            
            # If the percentage exceeds 10%, we fit the model
            if (perc_0 == 1) {
                use_prob <- rep(0, nrow(per_sum_eol))
            } else if (perc_0 >= .1) {
                
                # Get list of risk factors / subtypes again
                risk_facs <- names(per_sum_eol)[grep("^RF|^ST", names(per_sum_eol))]
                if (length(risk_facs) == 0) risk_facs <- NULL
                final_risk_facs <- c("eol_prob", risk_facs)
                
                # Fit the model and get the use probabilities
                formula.str <- paste0(colname, " ~ age + female + rec_enr + ", paste(final_risk_facs, collapse = " + "))
                use.typ.model <- glm(as.formula(formula.str), data = per_sum_eol, family = "binomial")
                use_prob <- predict(use.typ.model, newdata = per_sum_eol, type = "response")
            } else {
                cat("\tUse exceeds 90% for", choice, "so use model not fit - ")
            }
            
            ###
            ### Cost Model
            ###
            # Filter out only the episodes with non-zero cost
            per_sum_eol_filter <- per_sum_eol[per_sum_eol[,colname_cost] > 0, ]
            risk_facs <- names(per_sum_eol_filter)[grep("^RF|^ST", names(per_sum_eol_filter))]
            
            if (nrow(per_sum_eol_filter) > length(risk_facs)) {
                
                # Get the new list of risk factors and subtypes
                if (length(risk_facs) == 0) risk_facs <- NULL
                final_risk_facs <- c("eol_prob", risk_facs)
                
                # Fit the cost model
                formula.str <- paste0(colname_cost, " ~ age + female + rec_enr + ", paste(final_risk_facs, collapse = " + "))
                
                if (use_lasso) {
                    cost.model.las <- try(LASSO_Regression(data = per_sum_eol_filter, y = colname_cost, x = c("age", "female", "rec_enr", final_risk_facs)), silent = TRUE)
                    if (inherits(cost.model.las, "try-error")) {
                        cat("\tAll predictors in LASSO had zero variance - can't fit cost model")
                        return(NULL)
                    }
                    cost.model <- cost.model.las$model
                } else {
                    cost.model <- lm(as.formula(formula.str), data = per_sum_eol_filter)
                }
                
                if (use_lasso) {
                    cost.model.vif <- cost.model.las$model
                    my.coefnames <- rownames(coef(cost.model.vif))[-1]
                    my.coefs <- as.numeric(coef(cost.model.vif, s = "lambda.min")[,1])[-1]
                    my.stat <- NA
                    my.pv <- NA
                    my.vifs <- NA
                } else {
                    # Fit the VIF stuff
                    vif.terms <- c("age", "female", "rec_enr", final_risk_facs)
                    vif.terms <- vif.terms[which(!is.na(coef(cost.model)[-1]))]
                    
                    if (length(vif.terms) < 2) {
                        cat("\tNot enough model terms for VIF in", choice, "- can't fit cost model\n")
                        return(NULL) 
                    }
                    
                    formula.str.vif <- paste0(colname_cost, "~", paste(vif.terms, collapse = "+"))
                    cost.model.vif <- lm(as.formula(formula.str.vif), data = per_sum_eol_filter)
                    my.coefnames <- names(coef(cost.model.vif))[-1]
                    my.coefs <- coef(cost.model.vif)[-1]
                    my.stat <- summary(cost.model.vif)$coef[-1,3]
                    my.pv <- summary(cost.model.vif)$coef[-1,4]
                    
                    my.vifs <- NA
                    if (use_car) my.vifs <-  vif(cost.model.vif)
                }

                cost.model.diag <- data.frame(model = "cost", coef = my.coefnames, value = my.coefs, stat = my.stat, pvalue = my.pv, vif = my.vifs)
                
                # Create vectors to store the results
                my.list <- list(rep(0, nrow(per_sum_eol)), rep(0, nrow(per_sum_eol)), rep(0, nrow(per_sum_eol)))
                names(my.list) <- c(paste0("use_prob_", choice), paste0("cost_pred_", choice), paste0("exp_cost_", choice))
                
                # Compute predictions and expected costs as long as the model did not fail
                if (use_lasso) {
                    cost_pred <- rep(0, nrow(per_sum_eol))
                    names(cost_pred) <- 1:nrow(per_sum_eol)                        
                    cost_pred[names(cost_pred) %in% rownames(cost.model.las$predictions)] <- cost.model.las$predictions[,1]
                } else {
                    cost_pred <- predict(cost.model, newdata = per_sum_eol, type = "response")
                }
                exp_cost <- use_prob * cost_pred
                
                # Build a list of the use probability, predicted cost, and expected cost
                my.list <- list(use_prob, cost_pred, exp_cost)
                names(my.list) <- c(paste0("use_prob_", choice), paste0("cost_pred_", choice), paste0("exp_cost_", choice))
                
                if (use_lasso) my.f <- NA else my.f <- as.numeric(summary(cost.model)$fstat)
                if (use_lasso) my.rsq <- NA else my.rsq <- summary(cost.model)$r.squared
                if (use_lasso) my.pval <- NA else my.pval <- as.numeric(1 - pf(my.f[1], my.f[2], my.f[3]))
                if (use_lasso) {
                    my.cd <- NA
                } else {
                    my.cd <- cooks.distance(cost.model)
                    
                    cooks_d <- rep(0, nrow(per_sum_eol))
                    names(cooks_d) <- 1:nrow(per_sum_eol)                        
                    cooks_d[names(cooks_d) %in% names(my.cd)] <- my.cd
                    
                    my.cd <- cooks_d
                }

                my.fit <- cost_pred
                my.res <- per_sum_eol[,colname_cost] - cost_pred
                
                diag.return <- cbind(type = choice, rbind(use.model.diag, cost.model.diag))
                model.diag <- data.frame(type = choice, model = "cost", RSquared = my.rsq, FStat = my.f[1], PVal = my.pval)
                obs.diag <- data.frame(type = choice, model = "cost", obs = 1:nrow(per_sum_eol), member_id = per_sum_eol$member_id, cooks_distance = my.cd, fitted = my.fit, resid = my.res)
                use.diag <- data.frame(type = choice, model = "use", obs = 1:length(use_prob), use_prob = use_prob)
                
                tab.char <- if (perc_0 < .1) "" else "\t"
                cat(paste0(tab.char, "Successfully"), "fit cost model for", choice, "\n")
                
                if (use_lasso) names(my.coefs) <- my.coefnames
                
                # Return the coefficients as well as the expected costs
                return.list <- list(use = clean_coef(use.typ.model), cost = my.coefs, expected = my.list, diags = diag.return, model_diags = model.diag, obs_diags = obs.diag, use_diags = use.diag)
                return.list
            } else {
                cat("\tMore variables than observations for", choice, ", so cost model not fit\n")
            }
        })
        
        if (all(sapply(epi.results, is.null))) return(NULL)
        
        # Bind the epi id, eol probability, and the expected costs
        final.results <- cbind(epi_id = as.character(per_sum_eol$epi_id), eol_prob = per_sum_eol$eol_prob, as.data.frame(lapply(epi.results[!unlist(lapply(epi.results, is.null))], function(elem){elem$expected})))
        
        ## Coef Diags
        diag.final <- ldply(epi.results, function(test){test$diags})
        rownames(diag.final) <- NULL
        
        ## Model Diags
        model.diag.final <- ldply(epi.results, function(test){test$model_diags})
        
        ## Obs diags
        obs.diag.final <- ldply(epi.results, function(test){test$obs_diags})
        
        ## Use diags
        use.diag.final <- ldply(epi.results, function(test){test$use_diags})
        
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
        list(epi_number = epi.info$EPISODE_ID, epi_name = epi.info$EPISODE_ID, coefs = c(list(eol = clean_coef(eol.model)), my.coefs), expected = final.results, model_diags = model.diag.final, coef_diags = diag.final, obs_diags = obs.diag.final, use_diags = use.diag.final)
    } else {
        cat("\tNo data for episode", as.character(epi.info$EPISODE_ID), "\n")
    }
})

## Name each element of the list according to the proper episode name
names(epi.models) <- data.dictionary$EPISODE_ID

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
if (output == "db") {
    if (dbtype == "vRODBC") {
        # Write Coefficients and Expected Cost Files back to SQL database
        sqlSave(con, dat = final_expected, tablename = "ra_expected")
        sqlSave(con, dat = final_coefs, tablename = "ra_coeffs")
        
        # Optional- Write wide data file used to build models to SQL DB
        sqlSave(con, dat = final_wide_data, tablename = "ra_final_data")
    } else {
        dbWriteTable(con, name = "ra_expected", value = final_expected)
        dbWriteTable(con, name = "ra_coeffs", value = final_coefs)
        dbWriteTable(con, name = "ra_final_data", value = final_wide_data)
    }
} else if (output == "csv") {
    write.csv(final_expected, file = "ra_expected.csv")
    write.csv(final_coefs, file = "ra_coeffs.csv")
    write.csv(final_wide_data, file = "ra_final_data.csv")
}

##
## Diagnostics
##
final_model_diags <- ldply(epi.models, function(model) { cbind(epi_number = model$epi_number, epi_name = model$epi_name, model_diags = model$model_diags) })
final_model_diags[,1] <- NULL
names(final_model_diags) <- c("epi_number", "epi_name", "dependent_var", "model_type", "r_squared", "f_stat", "p_value")

final_coef_diags <- ldply(epi.models, function(model) { cbind(epi_number = model$epi_number, epi_name = model$epi_name, coef_diags = model$coef_diags) })
final_coef_diags[,1] <- NULL
names(final_coef_diags) <- c("epi_number", "epi_name", "dependent_var", "model_type", "parameter", "coef", "t_stat", "p_value", "vif")

final_obs_diags <- ldply(epi.models, function(model) { cbind(epi_number = model$epi_number, epi_name = model$epi_name, obs_diags = model$obs_diags) })
final_obs_diags[,1] <- NULL
names(final_obs_diags) <- c("epi_number", "epi_name", "dependent_var", "model_type", "obs", "member_id", "cooks_distance", "fitted", "resid")

final_use_diags <- ldply(epi.models, function(model){ cbind(epi_number = model$epi_number, epi_name = model$epi_name, use_diags = model$use_diags) })
final_use_diags[,1] <- NULL
names(final_use_diags) <- c("epi_number", "epi_name", "dependent_var", "model_type", "obs", "use_prob")

if (output == "db") {
    if (dbtype == "vRODBC") {
        sqlSave(con, dat = final_model_diags, tablename = "ra_model_diags")
        sqlSave(con, dat = final_coef_diags, tablename = "ra_coef_diags")
        sqlSave(con, dat = final_obs_diags, tablename = "ra_obs_diags")
        sqlSave(con, dat = final_use_diags, tablename = "ra_use_diags")
    } else {
        dbWriteTable(con, name = "ra_model_diags", value = final_model_diags)
        dbWriteTable(con, name = "ra_coef_diags", value = final_coef_diags)
        dbWriteTable(con, name = "ra_obs_diags", value = final_obs_diags)
        dbWriteTable(con, name = "ra_use_diags", value = final_use_diags)
    }
} else if (output == "csv") {
    write.csv(final_model_diags, file = "ra_model_diags.csv")
    write.csv(final_coef_diags, file = "ra_coef_diags.csv")
    write.csv(final_obs_diags, file = "ra_obs_diags.csv")
    write.csv(final_use_diags, file = "ra_use_diags.csv")
}

###
### END
###
