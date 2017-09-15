library(dplyr)
library(datapkg)
library(tidyr)

##################################################################
#
# Processing Script for Liquor Law Arrests
# Created by Jenna Daly
# On 09/12/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
raw_location <- grep("raw", sub_folders, value=T)
path_to_raw <- (paste0(getwd(), "/", raw_location))
raw_data <- dir(path_to_raw, recursive=T, pattern = "Crime") 

#Create new population data set for all years
source('./scripts/getPopulation.R')

ll_arrests <- data.frame(stringsAsFactors=F)
for (i in 1:length(raw_data)) {
  current_file <- read.csv(paste0(path_to_raw, "/", raw_data[i]), stringsAsFactors = F, header=T, check.names=F)
  names(current_file)[1] <- "Crime"
  #Remove rows without Ages
  current_file <- current_file[grepl("[0-9]", current_file$Crime),]
  #Isolate "liquor" rows
  current_file <- current_file[grep("Liquor", current_file$Crime),]
  #convert wide to long
  last_col <- ncol(current_file)
  current_file_long <- gather(current_file, Indicator, Value, 2:last_col, factor_key=TRUE)
  #Assign Age column
  current_file_long$Age <- gsub("([a-zA-Z ]+)(<?[0-9+-]+$)", "\\2", current_file_long$Crime)
  #Remove Ages from Crime column
  current_file_long$Crime <- gsub("[^a-zA-Z]", "", current_file_long$Crime)
  get_year <- as.numeric(substr(unique(unlist(gsub("[^0-9]", "", unlist(raw_data[i])), "")), 1, 4))
  current_file_long$Year <- get_year
  ll_arrests <- rbind(ll_arrests, current_file_long)
}

ll_arrests$Indicator <- as.character(ll_arrests$Indicator)

#Removing values for county (where name is both a county and a town)
years <- c("2010", "2011", "2012", "2013") #Years where indicators are counties not towns
indicators <- c("Hartford", "Windham", "Tolland", "New London", "New Haven", "Litchfield", "Fairfield")
ll_arrests <- ll_arrests[!(ll_arrests$Year %in% years & ll_arrests$Indicator %in% indicators),]

#Fix Indicator names
ll_arrests$Indicator[ll_arrests$Indicator == "CT"] <- "Connecticut"
ll_arrests$Indicator <- gsub(" CSP", "", ll_arrests$Indicator)
ll_arrests$Indicator <- gsub(" PD", "", ll_arrests$Indicator)

#Merge Groton names and Putnam names
ll_arrests$Indicator[which(grepl("Groton", ll_arrests$Indicator))] <- "Groton"
ll_arrests$Indicator[which(grepl("Putnam", ll_arrests$Indicator))] <- "Putnam"

#Merge in FIPS (to remove non-towns)
names(ll_arrests)[names(ll_arrests) == "Indicator"] <- "Town"

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

fips <- as.data.frame(fips)

ll_arrests_fips <- merge(ll_arrests, fips, by = "Town", all.y=T)

#Aggregate towns
ll_arrests_fips <- ll_arrests_fips %>% 
  group_by(Year, Age, Town, Crime) %>% 
  mutate(Value = sum(Value))

ll_arrests_fips <- unique(ll_arrests_fips)

# Assign age group flags (one age may belong to mutliple groups)
ll_arrests_fips$`Over 9 years flag` <- "TRUE"
ll_arrests_fips$`10 to 20 years flag` <- "FALSE"
ll_arrests_fips$`Over 20 years flag` <- "FALSE"
ll_arrests_fips$`18 to 24 years flag` <- "FALSE" 

#Assign flags based on Age column
ll_arrests_fips$`Over 9 years flag`[ll_arrests_fips$`Age` == "<10"] <- "FALSE"

x1020 <- c("10-12", "13-14", "15", "16", "17", "18", "19", "20")
ll_arrests_fips$`10 to 20 years flag`[ll_arrests_fips$`Age` %in% x1020] <- "TRUE"

over20 <- c("21", "22", "23", "24", "25-29", "30-34", "35-39", "40-44", 
            "45-49", "50-54", "55-59", "60-64", "65+")
ll_arrests_fips$`Over 20 years flag`[ll_arrests_fips$`Age` %in% over20] <- "TRUE"

x1824 <- c("18", '19', "20", "21", "22", "23", "24")
ll_arrests_fips$`18 to 24 years flag`[ll_arrests_fips$`Age` %in% x1824] <- "TRUE"

#Aggregate age groups based on flag
test <- ll_arrests_fips %>% 
  group_by(Town, Year, `Over 9 years flag`) %>% 
  mutate(`Over 9 years` = ifelse(`Over 9 years flag` == "TRUE", sum(Value), 0))

test <- test %>% 
  group_by(Town, Year, `10 to 20 years flag`) %>% 
  mutate(`10 to 20 years` = ifelse(`10 to 20 years flag` == "TRUE", sum(Value), 0))

test <- test %>% 
  group_by(Town, Year, `Over 20 years flag`) %>% 
  mutate(`Over 20 years` = ifelse(`Over 20 years flag` == "TRUE", sum(Value), 0))

test <- test %>% 
  group_by(Town, Year, `18 to 24 years flag`) %>% 
  mutate(`18 to 24 years` = ifelse(`18 to 24 years flag` == "TRUE", sum(Value), 0))

#Create total column
test <- test %>% 
  group_by(Town, Year) %>% 
  mutate(Total = sum(Value))

#Complete df with all totals
ll_arrests_totals <- test %>% 
  group_by(Town, Year, FIPS) %>% 
  summarise(`Over 9 years` = max(`Over 9 years`), 
         `10 to 20 years` = max(`10 to 20 years`), 
         `Over 20 years` = max(`Over 20 years`), 
         `18 to 24 years` = max(`18 to 24 years`), 
         `Total` = max(Total))
            
##########################################################################################################
#Create CT values for 2015 (2015 file does not have CT level values)
CT_2015 <- ll_arrests_fips[ll_arrests_fips$Year == "2015",]

CT_2015 <- as.data.frame(CT_2015)

total_2015 <- CT_2015 %>% 
  group_by(Age) %>% 
  summarise(Value = sum(Value))

# Assign age group flags (one age may belong to mutliple groups)
total_2015$`Over 9 years flag` <- "TRUE"
total_2015$`10 to 20 years flag` <- "FALSE"
total_2015$`Over 20 years flag` <- "FALSE"
total_2015$`18 to 24 years flag` <- "FALSE" 

#Assign flags based on Age column
total_2015$`Over 9 years flag`[total_2015$`Age` == "<10"] <- "FALSE"

x1020 <- c("10-12", "13-14", "15", "16", "17", "18", "19", "20")
total_2015$`10 to 20 years flag`[total_2015$`Age` %in% x1020] <- "TRUE"

over20 <- c("21", "22", "23", "24", "25-29", "30-34", "35-39", "40-44", 
            "45-49", "50-54", "55-59", "60-64", "65+")
total_2015$`Over 20 years flag`[total_2015$`Age` %in% over20] <- "TRUE"

x1824 <- c("18", '19', "20", "21", "22", "23", "24")
total_2015$`18 to 24 years flag`[total_2015$`Age` %in% x1824] <- "TRUE"

#Aggregate age groups based on flag
total_2015_test <- total_2015 %>% 
  group_by(`Over 9 years flag`) %>% 
  mutate(`Over 9 years` = ifelse(`Over 9 years flag` == "TRUE", sum(Value), 0))

total_2015_test <- total_2015_test %>% 
  group_by(`10 to 20 years flag`) %>% 
  mutate(`10 to 20 years` = ifelse(`10 to 20 years flag` == "TRUE", sum(Value), 0))

total_2015_test <- total_2015_test %>% 
  group_by(`Over 20 years flag`) %>% 
  mutate(`Over 20 years` = ifelse(`Over 20 years flag` == "TRUE", sum(Value), 0))

total_2015_test <- total_2015_test %>% 
  group_by(`18 to 24 years flag`) %>% 
  mutate(`18 to 24 years` = ifelse(`18 to 24 years flag` == "TRUE", sum(Value), 0))

total_2015_test <- as.data.frame(total_2015_test)

#Create total column
total_2015_test <- total_2015_test %>% 
  mutate(Total = sum(Value))

total_2015_test <- as.data.frame(total_2015_test)

#CT df with all totals
total_2015_test <- total_2015_test %>% 
  summarise(`Over 9 years` = max(`Over 9 years`), 
            `10 to 20 years` = max(`10 to 20 years`), 
            `Over 20 years` = max(`Over 20 years`), 
            `18 to 24 years` = max(`18 to 24 years`), 
            `Total` = max(Total))

#Create columns
total_2015_test$Town <- "Connecticut"
total_2015_test$FIPS <- "09"
total_2015_test$Year <- 2015

ll_arrests_totals <- as.data.frame(ll_arrests_totals)

#Merge CT 2015 with rest of data
ll_arrests_totals <- rbind(ll_arrests_totals, total_2015_test)

#convert wide to long
ll_arrests_totals_long <- gather(ll_arrests_totals, `Age Range`, Value, 4:8, factor_key=TRUE)
####################################################################################################

## read population data for denominators in rate calculations
pops <- read.csv(paste0(path_to_raw, "/", "populations.csv"), stringsAsFactors = F, header=T, check.names=F)

# Helper function for MOE
calcMOE <- function(x, y, moex, moey) {
  moex2 <- moex^2
  moey2 <- moey^2
  d <- x/y
  d2 <- d^2
  
  radicand <- ifelse(
    moex2 < (d2 * moey2),
    moex2 + (d2 * moey2),
    moex2 - (d2 * moey2)
  )
  
  return(sqrt(radicand)/y)
}

pops$FIPS <- gsub("^", "0", pops$FIPS)

percents <- merge(ll_arrests_totals_long, pops, by = c("Year", "Age Range", "FIPS"))

#Rates are calculated per 10000
percents <- percents %>% 
  mutate(Pop = (Pop/1e4), 
         MOE = (MOE/1e4))

# calculate rates with population denominators,
# keep MOES, calculating appropriately
percents <- percents %>% 
  mutate(`Liquor Law Arrests` = round((Value / Pop), 2),
         `Margins of Error` = round((calcMOE(Value, Pop, 0, MOE)), 2),
         `Measure Type` = "Rate (per 10,000)")

nulls <- c("Value", "Pop", "MOE")
percents[nulls] <- NULL

# melt percents
percents <- melt(
  percents,
  id.vars = c("Town", "FIPS", "Year", "Age Range", "Measure Type"),
  variable.name = "Variable",
  variable.factor = F,
  value.name = "Value",
  value.factor = F
)

percents$Variable <- as.character(percents$Variable)

## FINAL STEPS
# add extra data
ll_arrests_totals_long$`Measure Type` <- "Number"
ll_arrests_totals_long$Variable <- "Liquor Law Arrests"

percents <- as.data.frame(percents)
ll_arrests_totals_long <- as.data.frame(ll_arrests_totals_long)

# combine number and rate measures into one dataset
ll_arrests_complete <- rbind(ll_arrests_totals_long, percents)

#Assign factors for sorting
ll_arrests_complete$`Age Range` <- factor(ll_arrests_complete$`Age Range`, levels = c("Total", "Over 9 years", "10 to 20 years", "18 to 24 years", "Over 20 years"))

# Order and sort columns
ll_arrests_complete <- ll_arrests_complete %>% 
  select(Town, FIPS, Year, `Age Range`, `Measure Type`, Variable, Value) %>% 
  arrange(Town, Year, `Age Range`, `Measure Type`)

# Write to File
write.table(
  ll_arrests_complete,
  file.path(getwd(), "data", "liquor-law-arrests_2015.csv"),
  sep = ",",
  row.names = F,
  na = "-9999"
)

