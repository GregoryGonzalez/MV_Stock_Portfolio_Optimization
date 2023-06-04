#ISDS 570 Group Project
rm(list=ls(all=T)) # this just removes everything from memory
# Load CSV Files ----------------------------------------------------------

# Load daily prices from CSV - no parameters needed
dp<-read.csv('C:/Users/ggonz/Desktop/ISDS 570/ISDS 570 Group Project/daily_prices_2016_2021.csv') # no arguments

#Explore
head(dp) #first few rows
tail(dp) #last few rows

dp<-head(dp,-1)


rm(dp) # remove from memory


# Connect to PostgreSQL ---------------------------------------------------


require(RPostgres) # did you install this package?
require(DBI)
conn <- dbConnect(RPostgres::Postgres()
                 ,user="groupprojectreader"
                 ,password="read123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="Group_Project"
)

#custom calendar
qry<-"SELECT * FROM custom_calendar WHERE date BETWEEN '2016-01-01' AND '2021-03-26'"
ccal<-dbGetQuery(conn,qry)
#eod prices and indices
qry1="SELECT symbol,date,adj_close FROM eod_indices WHERE date BETWEEN '2016-01-01' AND '2021-03-26'"
qry2="SELECT ticker,date,adj_close FROM eod_quotes WHERE date BETWEEN '2016-01-01' AND '2021-03-26'"
eod<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
dbDisconnect(conn)
rm(conn)

#Explore
head(ccal)
tail(ccal)
nrow(ccal)

head(eod)
tail(eod)
nrow(eod)

head(eod[which(eod$symbol=='SP500TR'),])
tail(eod[which(eod$symbol=='SP500TR'),])


# Use Calendar --------------------------------------------------------

tdays<-ccal[which(ccal$trading==1),,drop=F]
tail(tdays)
nrow(tdays)-1 #trading days between 2015 and 2020

# Completeness ----------------------------------------------------------
# Percentage of completeness
pct<-table(eod$symbol)/(nrow(tdays)-1)
selected_symbols_daily<-names(pct)[which(pct>=0.99)]
eod_complete<-eod[which(eod$symbol %in% selected_symbols_daily),,drop=F]

#check
head(eod_complete)
tail(eod_complete)
nrow(eod_complete)

# Transform (Pivot) -------------------------------------------------------

require(reshape2) 
eod_pvt<-dcast(eod_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)
#check
eod_pvt[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt) # column count
nrow(eod_pvt)

# Merge with Calendar -----------------------------------------------------
eod_pvt_complete<-merge.data.frame(x=tdays[,'date',drop=F],y=eod_pvt,by='date',all.x=T)

#check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

#use dates as row names and remove the date column
rownames(eod_pvt_complete)<-eod_pvt_complete$date
eod_pvt_complete$date<-NULL #remove the "date" column

#re-check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

# Missing Data Imputation -----------------------------------------------------
require(zoo)
eod_pvt_complete<-na.locf(eod_pvt_complete,na.rm=F,fromLast=F,maxgap=3)
#re-check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

# Calculating Returns -----------------------------------------------------
require(PerformanceAnalytics)
eod_ret<-CalculateReturns(eod_pvt_complete)

#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)

#remove the first row
eod_ret<-tail(eod_ret,-1) #use tail with a negative value
#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)

# YOUR TURN: calculate eom_ret (monthly returns)

# Check for extreme returns -------------------------------------------
# There is colSums, colMeans but no colMax so we need to create it
colMax <- function(data) sapply(data, max, na.rm = TRUE)
# Apply it
max_daily_ret<-colMax(eod_ret)
max_daily_ret[1:10] #first 10 max returns
# And proceed just like we did with percentage (completeness)
selected_symbols_daily<-names(max_daily_ret)[which(max_daily_ret<=1.00)]
length(selected_symbols_daily)

#subset eod_ret
eod_ret<-eod_ret[,which(colnames(eod_ret) %in% selected_symbols_daily),drop=F]
#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)


# Export data from R to CSV -----------------------------------------------
write.csv(eod_ret,'C:/Temp/eod_ret.csv')


# Tabular Return Data Analytics -------------------------------------------

# We will select 'SP500TR' and 12 RANDOM TICKERS

random12 <- c('KN','KNBWY','KNCAY','LNNNY','LYB','LTC','GPN','GRMN','GAIN','LAD','LAKE','LBRDA')
random12

# We need to convert data frames to xts (extensible time series)
Ra<-as.xts(eod_ret[,random12,drop=F])
Rb<-as.xts(eod_ret[,'SP500TR',drop=F]) #benchmark

head(Ra)
tail(Ra)
head(Rb)
tail(Rb)


# Stats
table.Stats(Ra)

# Distributions
table.Distributions(Ra)

# Returns
table.AnnualizedReturns(cbind(Rb,Ra),scale=252) # note for monthly use scale=12

# Accumulate Returns
acc_Ra<-Return.cumulative(Ra)
acc_Rb<-Return.cumulative(Rb)

# Capital Assets Pricing Model
table.CAPM(Ra,Rb)

# Graphical Return Data Analytics -----------------------------------------

# Cumulative returns chart
chart.CumReturns(head(Ra,-58),legend.loc = 'topleft') #1

chart.CumReturns(cbind(tail(Rb,58),tail(Ra,58)),legend.loc = 'topleft') #3
#Box plots
chart.Boxplot(cbind(Rb,Ra))

chart.Drawdown(Ra,legend.loc = 'bottomleft')


# MV Portfolio Optimization -----------------------------------------------

# withhold the last 253 trading days
Ra_training<-head(Ra,-58)
tail(Ra_training)
Rb_training<-head(Rb,-58)
tail(Rb_training)
# use the last 253 trading days for testing
Ra_testing<-tail(Ra,58)
head(Ra_testing)
Rb_testing<-tail(Rb,58)
tail(Rb_testing)

#optimize the MV (Markowitz 1950s) portfolio weights based on training
table.AnnualizedReturns(Rb_training)
mar<-mean(Rb_training) #we need daily minimum acceptable return

require(PortfolioAnalytics)
require(ROI) # make sure to install it
require(ROI.plugin.quadprog)  # make sure to install it
pspec<-portfolio.spec(assets=colnames(Ra_training))
pspec<-add.objective(portfolio=pspec,type="risk",name='StdDev')
pspec<-add.constraint(portfolio=pspec,type="full_investment")
pspec<-add.constraint(portfolio=pspec,type="return",return_target=mar)

#optimize portfolio
opt_p<-optimize.portfolio(R=Ra_training,portfolio=pspec,optimize_method = 'ROI')
opt_p
#extract weights (negative weights means shorting)
opt_w<-opt_p$weights
sum(opt_w)

#apply weights to test returns
Rp<-Rb_testing # easier to apply the existing structure
#define new column that is the dot product of the two vectors
Rp$ptf<-Ra_testing %*% opt_w

#check
head(Rp)
tail(Rp)

#Compare basic metrics
table.AnnualizedReturns(Rp)

# Chart Hypothetical Portfolio Returns ------------------------------------

chart.CumReturns(Rp,legend.loc = 'bottomright')
