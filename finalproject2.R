rm(list=ls())

args = (commandArgs(trailingOnly=TRUE))
if(length(args) == 1){
  RpackagesDir = args[1]
} else {
  cat('usage: Rscript finalproject.R <RpackagesDir>\n', file=stderr())
  stop()
}

library(doParallel)

# set up parallel computing
#cl = makeCluster(detectCores())
#registerDoParallel(cl)
#print(cl)

# Tell R to search in RpackagesDir, in addition to where it already
# was searching, for installed R packages.
.libPaths(new=c(RpackagesDir, .libPaths()))
if (!require("arrow")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="arrow",repos = "http://cran.us.r-project.org")
  stopifnot(require("arrow")) # If loading still fails, quit.
}

if (!require("ggplot2")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="ggplot2",repos = "http://cran.us.r-project.org")
  stopifnot(require("ggplot2")) # If loading still fails, quit.
}

if (!require("ggthemes")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="ggthemes",repos = "http://cran.us.r-project.org")
  stopifnot(require("ggthemes")) # If loading still fails, quit.
}

if (!require("dplyr")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="dplyr",repos = "http://cran.us.r-project.org")
  stopifnot(require("dplyr")) # If loading still fails, quit.
}


if (!require("lubridate")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="lubridate",repos = "http://cran.us.r-project.org")
  stopifnot(require("lubridate")) # If loading still fails, quit.
}

if (!require("tidyr")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="tidyr",repos = "http://cran.us.r-project.org")
  stopifnot(require("tidyr")) # If loading still fails, quit.
}

if (!require("DT")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="DT",repos = "http://cran.us.r-project.org")
  stopifnot(require("DT")) # If loading still fails, quit.
}


if (!require("caret")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="caret",repos = "http://cran.us.r-project.org")
  stopifnot(require("caret")) # If loading still fails, quit.
}

if (!require("scales")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="caret",repos = "http://cran.us.r-project.org")
  stopifnot(require("caret")) # If loading still fails, quit.
}

if (!require("data.table")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="data.table",repos = "http://cran.us.r-project.org")
  stopifnot(require("data.table")) # If loading still fails, quit.
}

if (!require("FNN")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="FNN",repos = "http://cran.us.r-project.org")
  stopifnot(require("FNN")) # If loading still fails, quit.
}

if (!require("rattle")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="rattle",repos = "http://cran.us.r-project.org")
  stopifnot(require("rattle")) # If loading still fails, quit.
}

if (!require("rpart")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="rpart",repos = "http://cran.us.r-project.org")
  stopifnot(require("rpart")) # If loading still fails, quit.
}

if (!require("foreach")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="foreach",repos = "http://cran.us.r-project.org")
  stopifnot(require("foreach")) # If loading still fails, quit.
}

if (!require("doParallel")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="doParallel",repos = "http://cran.us.r-project.org")
  stopifnot(require("doParallel")) # If loading still fails, quit.
}
if (!require("randomForest")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="randomForest",repos = "http://cran.us.r-project.org")
  stopifnot(require("randomForest")) # If loading still fails, quit.
}
if (!require("class")) { # If loading package fails ...
  # Install package in RpackagesDir.
  install.packages(pkgs="class",repos = "http://cran.us.r-project.org")
  stopifnot(require("class")) # If loading still fails, quit.
}

library(ggplot2)
library(ggthemes)
library(lubridate)
library(dplyr)
library(tidyr)
library(DT)
library(caret)
library(scales)
library(data.table)
library(FNN)
library(rpart)
library(rattle)
library(foreach)
library(randomForest)
library(arrow)
library(class)


file_names = c('2012_1st.parquet', '2012_2nd.parquet', '2013_1st.parquet', '2013_2nd.parquet', "2014_1st.parquet", '2014_2nd.parquet', '2015_1st.parquet', '2015_2nd.parquet', '2016_1st.parquet','2016_2nd.parquet', '2017_1st.parquet', '2017_2nd.parquet', '2018.parquet', '2019.parquet', '2020.parquet', '2021.parquet')

# loop through the list of files 
df = foreach(file = file_names, .combine = rbind) %do% {
  # read the parquet file
  read_parquet(file)
}



# data processing
## fill missing values
df[is.na(df)] = 0
hist(df$passenger_count)

df$tpep_pickup_datetime = as.POSIXct(df$tpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
df$tpep_pickup_datetime = ymd_hms(df$tpep_pickup_datetime)

# Create individual columns for month day and year
df$day = factor(day(df$tpep_pickup_datetime))
df$month = factor(month(df$tpep_pickup_datetime))
df$year = factor(year(df$tpep_pickup_datetime))
df$dayofweek = factor(wday(df$tpep_pickup_datetime))
df$payment_type = factor(df$payment_type)
df$passenger_count = factor(df$passenger_count)
df$hour =factor(format(df$tpep_pickup_datetime, format = "%H"))

sample = sample(c(TRUE, FALSE), nrow(df), replace=TRUE, prob=c(0.7,0.3))
train  = df[sample, ]
test   = df[!sample, ]


# Creating a simple Linear Regression Model

lm_model = lm(total_amount ~ trip_distance + passenger_count + payment_type + month + dayofweek + hour , data=train)
summary(lm_model)
sink("/workspace/sli826/projectHPC/lm.txt")
print(summary(lm_model))
sink()  

eval_metrics = function(true, predicted){
  mse = mean((true-predicted)**2)
  rmse = sqrt(mse) 
  rmse = signif(rmse, digits = 5)
  
  SSE = sum((predicted - true)^2)
  SST = sum((true - mean(true))^2)
  R_square = 1 - SSE / SST
  R_square = signif(R_square, digits = 5)
  
  cat(paste0("RMSE: ", rmse, "\nR-Squared: ", R_square, "\n" ))
}

preds = predict(lm_model, test)

modelEval = cbind(test$total_amount, preds)
colnames(modelEval) = c('Actual', 'Predicted')
modelEval = as.data.frame(modelEval)

eval_metrics(modelEval$Actual, modelEval$Predicted)

######## decision tree
tree1 = rpart(total_amount ~ trip_distance + passenger_count + payment_type + month + dayofweek + hour + year, data=train,  method = 'anova')
pdf("tree_plot.pdf")
fancyRpartPlot(tree1, caption = "Regression Tree")
dev.off()
library(randomForest)
library(caret)


varimp.tree1 = varImp(tree1)
varimp.tree1

ggplot(varimp.tree1, aes(x = row.names(varimp.tree1), y = Overall)) +
  geom_bar(stat = "identity") +
  ggtitle("Variable Importance Scores") + xlab("Feature") 

tree1.pred = predict(tree1, newdata = test)

modelEval = cbind(test$total_amount, tree1.pred)
colnames(modelEval) = c('Actual', 'Predicted')
modelEval = as.data.frame(modelEval)

# Display Results
print(
  "decision tree: "
)
eval_metrics(modelEval$Actual, modelEval$Predicted)


###########KNN

x_train = train[, c("trip_distance", "passenger_count", "payment_type",'month', "year",'dayofweek', "hour")]
y_train = train[, c("total_amount")]
x_test =  test[, c("trip_distance", "passenger_count", "payment_type",'month', "year",'dayofweek', "hour")]
y_test = test[, c("total_amount")]

# extract the categorical variables
categorical_vars_train = train[, c("payment_type",'month','dayofweek', "hour", 'passenger_count', 'year')] 
one_hot_train = model.matrix(~ 0 + ., data = categorical_vars_train)
encoded_df_train = cbind(train[, c("trip_distance")], one_hot_train)

categorical_vars_test = test[, c("payment_type",'month','dayofweek', "hour", 'passenger_count', 'year')] 
one_hot_test = model.matrix(~ 0 + ., data = categorical_vars_test)
encoded_df_test = cbind(test[, c("trip_distance")], one_hot_test)

encoded_df_train[] = lapply(encoded_df_train, as.numeric)
encoded_df_test[] = lapply(encoded_df_test, as.numeric)

# Perform KNN regression with k=5
knn_result = knn.reg(train = encoded_df_train, test = encoded_df_test, y = y_train, k = 5)


modelEval = cbind(y_test$total_amount, knn_result$pred)
colnames(modelEval) = c('Actual', 'Predicted')
modelEval = as.data.frame(modelEval)

# Display Results
eval_metrics(modelEval$Actual, modelEval$Predicted)




#### random Forest

model = randomForest(total_amount ~ trip_distance + passenger_count + payment_type + month + year + dayofweek + hour , data=train, ntree = 5)
predictions = predict(model, newdata = test[, c("trip_distance", "passenger_count", "payment_type",'month', "year",'dayofweek', "hour")])
modelEval = cbind(test$total_amount, predictions)
colnames(modelEval) = c('Actual', 'Predicted')
modelEval = as.data.frame(modelEval)

# Display Results
eval_metrics(modelEval$Actual, modelEval$Predicted)


