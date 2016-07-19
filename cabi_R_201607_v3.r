
library(DBI)
library(RSQLite)
library(corrplot)
library(caret)
library(earth)
library(nnet)
library(kernlab)
library(randomForest)
library(gbm)
library(ggplot2)

con = dbConnect(RSQLite::SQLite(), dbname='cabi_201604.db')
alltables = dbListTables(con)
S0 = dbGetQuery( con,'select * from Stations' )
B0 = dbGetQuery( con,'select * from bikesMerged' )
D0 = dbGetQuery( con,'select * from docksMerged' )
dbDisconnect(con)

responseTypes = c('B','D')


# Just focus on bikes, not docks, from here out


memberTypes = c('C','M')
stations = S0["terminalname"]

subsets = c("all","casual","member","Arlington",31258,31200)

responseCols = vector("list",length(subsets))
names(responseCols) <- subsets
responseCols$all <- grep("B_.*",names(B0))
responseCols$casual <- grep("B_C_.*",names(B0))
responseCols$member <- grep("B_M_.*",names(B0))
responseCols$Arlington <- unlist(lapply(S0$terminalname[S0$juris=="Arlington"],function(x) grep(paste('B_._',toString(x),sep=''),names(B0))))
responseCols$"31258" <- grep("B_._31258",names(B0))	# Lincoln Memorial station
responseCols$"31200" <- grep("B_._31200",names(B0))	# Dupont Circle station

timeW_Column = grep("timeW",names(B0))	# timeW === number of seconds since unix time 0 (from the matched weather reading)
index_Column = grep("index",names(B0))  	# index === number of hours since unix time 0

#pairs(B0[,-c(responseCols$all,timeW_Column,index_Column)])
#cor(B0[,-c(responseCols$all,timeW_Column,index_Column)])

# Observations:
	# high correlations between:
		# tempF and dewpoint
		# RH and dewpoint
	# but there is a low correlation between tempF and RH
			# Therefore dewpoint not likely to be a useful predictorColumn. Throw it out
	# I don't want two precip columns:
		# Select precip01h
	# Snow depth might be slightly helpful, but it is a near-zero variance predictor, and it will complicate things for this project.
		# So remove it
dewpointColumn = grep("dewpointF",names(B0))
precip06hColumn = grep("precip06h",names(B0))
snowDepthColumn = grep("snowDepth",names(B0))
predictorCols = B0[,-c(responseCols$all,timeW_Column,index_Column,dewpointColumn,precip06hColumn,snowDepthColumn)]

# make a separate data frame for each subset:
BB = setNames(vector("list", length(subsets)), subsets)
BB2 = setNames(vector("list", length(subsets)), subsets)
goodyears = ((B0[,"year"]>=2014) & (B0[,"year"]<=2015))
for(N in names(BB))
{
	alltime_response = rowSums(B0[,responseCols[[N]]])
	BB[[N]] = predictorCols[goodyears,]
	BB[[N]]$response = alltime_response[which(goodyears)]
	BB2[[N]] = BB[[N]]
	BB2[[N]]$T_minus_003 = alltime_response[sapply(which(goodyears),function(x) (x-003))]
	BB2[[N]]$T_minus_024 = alltime_response[sapply(which(goodyears),function(x) (x-024))]
	BB2[[N]]$T_minus_168 = alltime_response[sapply(which(goodyears),function(x) (x-168))]
}

# create train/test splits:
set.seed(123)
trainSetDexes <- createDataPartition(1:(length(BB[[N]]$response)),p=0.8,list=FALSE)
BB_train <- setNames(vector("list", length(subsets)), subsets)
BB_test <- setNames(vector("list", length(subsets)), subsets)
BB2_train <- setNames(vector("list", length(subsets)), subsets)
BB2_test <- setNames(vector("list", length(subsets)), subsets)
for(N in names(BB))
{
	BB_train[[N]] = BB[[N]][trainSetDexes,]
	BB_test[[N]]  = BB[[N]][-trainSetDexes,]
	BB2_train[[N]] = BB2[[N]][trainSetDexes,]
	BB2_test[[N]]  = BB2[[N]][-trainSetDexes,]
}



# Apply 10-fold CrossVal to the training set:
set.seed(123)
ctrl <- trainControl(method = "cv", number = 10)
preProc_Arguments = c("BoxCox","center","scale") 	# center and scale before applying TRAIN function




# RANDOM FOREST: 

# without autocorrelation predictors
rafoGrid = expand.grid(.mtry=c(3))
rafoModel_B = setNames(vector("list", length(subsets)), subsets)
rafoPredTrain_B = setNames(vector("list", length(subsets)), subsets)
rafoPostResampleTrain_B = setNames(vector("list", length(subsets)), subsets)
rafoPredTest_B = setNames(vector("list", length(subsets)), subsets)
rafoPostResampleTest_B = setNames(vector("list", length(subsets)), subsets)
for(N in names(BB))
{
	print(sprintf('Executing Random Forest, no autocor, for response type: %s',N))
	t0=proc.time()
	set.seed(123)
	rafoModel_B[[N]] <- train(response ~ .,data=BB_train[[N]],method="rf",tuneGrid=rafoGrid,ntree=500,importance=TRUE)
	print(proc.time()-t0)
	rafoPredTrain_B[[N]] <- predict(rafoModel_B[[N]],newdata=BB_train[[N]])
	rafoPostResampleTrain_B[[N]] = postResample(pred=rafoPredTrain_B[[N]],obs=BB_train[[N]]$response)
	rafoPredTest_B[[N]] <- predict(rafoModel_B[[N]],newdata=BB_test[[N]])
	rafoPostResampleTest_B[[N]] = postResample(pred=rafoPredTest_B[[N]],obs=BB_test[[N]]$response)
}

# with autocorrelation predictors:
rafoGrid = expand.grid(.mtry=c(4))
rafo2Model_B = setNames(vector("list", length(subsets)), subsets)
rafo2PredTrain_B = setNames(vector("list", length(subsets)), subsets)
rafo2PostResampleTrain_B = setNames(vector("list", length(subsets)), subsets)
rafo2PredTest_B = setNames(vector("list", length(subsets)), subsets)
rafo2PostResampleTest_B = setNames(vector("list", length(subsets)), subsets)
for(N in names(BB))
{
	print(sprintf('Executing Random Forest, with autocor, for response type: %s',N))
	t0=proc.time()
	set.seed(123)
	rafo2Model_B[[N]] <- train(response ~ .,data=BB2_train[[N]],method="rf",tuneGrid=rafoGrid,ntree=500,importance=TRUE)
	print(proc.time()-t0)
	rafo2PredTrain_B[[N]] <- predict(rafo2Model_B[[N]],newdata=BB2_train[[N]])
	rafo2PostResampleTrain_B[[N]] = postResample(pred=rafo2PredTrain_B[[N]],obs=BB2_train[[N]]$response)
	rafo2PredTest_B[[N]] <- predict(rafo2Model_B[[N]],newdata=BB2_test[[N]])
	rafo2PostResampleTest_B[[N]] = postResample(pred=rafo2PredTest_B[[N]],obs=BB2_test[[N]]$response)
}

# I previously ran similar training and cross-validation on Neural Networks, SVM, MARS, and KNN 
	# but I had the most success with Random Forests (as measured by RMSE and R2 metrics)

# Plot 
O_vs_P_allBikes_Test <- data.frame(rafo2PredTest_B[["all"]],BB2_test[["all"]]$response)
names(O_vs_P_allBikes_Test) <- c("predicted","observed")

pdf("rafoFIT.pdf")
g1 = ggplot(O_vs_P_allBikes_Test,aes(x=predicted,y=observed))+geom_point(color="firebrick")
g1 <- g1 + ggtitle('Random Forest Model \n Test Set: Hourly Number of Rides System-Wide')
g1 <- g1 + theme(plot.title = element_text(size=18, face="bold"))
g1 <- g1 + xlim(c(0,1700)) + ylim(c(0,1700))
g1 <- g1 + theme(axis.title.x = element_text(size=16),axis.title.y = element_text(size=16))
g1
dev.off()

