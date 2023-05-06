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

library(arrow)

df = read_parquet('/Users/xnl/Downloads/newdata/2021.parquet')

# data processing
## fill missing values
df[is.na(df)] = 0

df$tpep_pickup_datetime = as.POSIXct(df$tpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
df$tpep_pickup_datetime = ymd_hms(df$tpep_pickup_datetime)

# Create individual columns for month day and year
df$day = factor(day(df$tpep_pickup_datetime))
df$month = factor(month(df$tpep_pickup_datetime))
df$year = factor(year(df$tpep_pickup_datetime))
df$dayofweek = factor(wday(df$tpep_pickup_datetime))
df$payment_type = factor(df$payment_type)
df$hour = factor(format(df$tpep_pickup_datetime, format = "%H"))

#use 70% of dataset as training set and 30% as test set
sample = sample(c(TRUE, FALSE), nrow(df), replace=TRUE, prob=c(0.7,0.3))
train  = df[sample, ]
test   = df[!sample, ]

temp = train

# distribution
hour_data = temp %>%
  group_by(hour) %>%
  dplyr::summarize(Total = n(), total_fare = sum(total_amount), avg_fare = mean(total_amount), avg_tips = mean(tip_amount), total_tips = sum(tip_amount)) 
hour_data
write.table(hour_data, file = "hour_data.csv", sep=',')


pdf("hour_data.pdf")
ggplot(hour_data, aes(hour, Total)) + 
  geom_bar( stat = "identity", fill = "steelblue") +
  ggtitle("Trips Every Hour") +
  theme(legend.position = "none") +
  scale_y_continuous(labels = comma)
dev.off()

pdf("hour_fare.pdf")
ggplot(hour_data, aes(hour, total_fare)) + 
  geom_bar( stat = "identity", fill = "steelblue") +
  ggtitle("Total Taxi Fare Every Hour") +
  theme(legend.position = "none") +
  scale_y_continuous(labels = comma)
dev.off()

pdf("avg_hour_fare.pdf")
ggplot(hour_data, aes(hour, avg_fare)) + 
  geom_bar( stat = "identity", fill = "steelblue") +
  ggtitle("Average Taxi Fare Every Hour") +
  theme(legend.position = "none") +
  scale_y_continuous(labels = comma)
dev.off()

pdf("avg_tips.pdf")
ggplot(hour_data, aes(hour, avg_tips)) + 
  geom_bar( stat = "identity", fill = "steelblue") +
  ggtitle("Average Tips Every Hour") +
  theme(legend.position = "none") +
  scale_y_continuous(labels = comma)
dev.off()

pdf("total_tips.pdf")
ggplot(hour_data, aes(hour, total_tips)) + 
  geom_bar( stat = "identity", fill = "steelblue") +
  ggtitle("Total Tips Every Hour") +
  theme(legend.position = "none") +
  scale_y_continuous(labels = comma)
dev.off()

# Aggregate the data by month and hour
month_hour_data = temp %>% group_by(month, hour) %>%  dplyr::summarize(Total = n(), total_fare = sum(total_amount), avg_fare = mean(total_amount), avg_tips = mean(tip_amount), total_tips = sum(tip_amount)) 
write.table(month_hour_data, file = "month_hour_data.csv", sep=',')

pdf("month_hour_data.pdf")
ggplot(month_hour_data, aes(hour, Total, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Trips by Hour and Month") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_hour_total_fare.pdf")
ggplot(month_hour_data, aes(hour, total_fare, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Total Taxi Fare by Hour and Month") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_hour_avg_fare.pdf")
ggplot(month_hour_data, aes(hour, avg_fare, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Average Taxi Fare by Hour and Month") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_hour_total_tips.pdf")
ggplot(month_hour_data, aes(hour, total_tips, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Total Taxi tips by Hour and Month") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_hour_avg_tips.pdf")
ggplot(month_hour_data, aes(hour, avg_tips, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Average Taxi tips by Hour and Month") + 
  scale_y_continuous(labels = comma)
dev.off()

### Aggregate the data by month and dayofweek
month_dayofweek_data = temp %>% group_by(month, dayofweek) %>%  dplyr::summarize(Total = n(), total_fare = sum(total_amount), avg_fare = mean(total_amount), avg_tips = mean(tip_amount), total_tips = sum(tip_amount)) 

pdf("month_dayofweek_data.pdf")
ggplot(month_dayofweek_data, aes(dayofweek, Total, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Trips by Hour and Day of Week") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_dayofweek_total_fare.pdf")
ggplot(month_dayofweek_data, aes(dayofweek, total_fare, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Total Taxi Fare by Hour and Day of Week") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_dayofweek_avg_fare.pdf")
ggplot(month_dayofweek_data, aes(dayofweek, avg_fare, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Average Taxi Fare by Hour and Day of Week") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_dayofweek_total_tips.pdf")
ggplot(month_dayofweek_data, aes(dayofweek, total_tips, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Total Taxi tips by Hour and Day of Week") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("month_dayofweek_avg_tips.pdf")
ggplot(month_dayofweek_data, aes(dayofweek, avg_tips, fill=month)) + 
  geom_bar(stat = "identity") + 
  ggtitle("Average Taxi tips by Hour and Day of Week") + 
  scale_y_continuous(labels = comma)
dev.off()

pdf("trip_distance_total_amount.pdf")
ggplot(temp, aes(trip_distance, total_amount)) +
  geom_point(color="light blue") + 
  coord_cartesian(ylim = c(0, 300), xlim = c(0, 60))
dev.off()

pdf("payment_type_total_amount.pdf")
ggplot(temp, aes(payment_type, total_amount)) +
  geom_boxplot(outlier.colour="light blue") +
  coord_cartesian(ylim = c(0, 50))
dev.off()

pdf("trip_distance_tip_amount.pdf")
ggplot(temp, aes(trip_distance, tip_amount)) +
  geom_point(color="light blue") +
  coord_cartesian(ylim = c(0, 100), xlim = c(0, 60))
dev.off()

pdf("payment_type_tip_amount.pdf")
ggplot(temp, aes(payment_type, tip_amount)) +
  geom_boxplot(outlier.colour="light blue") +
  coord_cartesian(ylim = c(0, 5))
dev.off()

pdf("passenger_count_tip_amount.pdf")
ggplot(temp, aes(passenger_count, tip_amount)) +
  geom_boxplot(outlier.colour="light blue") +
  coord_cartesian(ylim = c(0, 5))
dev.off()

pdf("passenger_count_total_amount.pdf")
ggplot(temp, aes(passenger_count, tip_amount)) +
  geom_boxplot(outlier.colour="light blue") +
  coord_cartesian(ylim = c(0, 100))
dev.off()









