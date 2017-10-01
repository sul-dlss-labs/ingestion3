#!/bin/bash
# 
#  .----------------.  .----------------.   
# | .--------------. || .--------------. |  
# | |  ________    | || |   ______     | |  
# | | |_   ___ `.  | || |  |_   __ \   | |  
# | |   | |   `. \ | || |    | |__) |  | |  
# | |   | |    | | | || |    |  ___/   | |  
# | |  _| |___.' / | || |   _| |_      | |  
# | | |________.'  | || |  |_____|     | |  
# | |              | || |              | |  
# | '--------------' || '--------------' |  
#  '----------------'  '----------------'   
#  .----------------.  .----------------.   
# | .--------------. || .--------------. |  
# | |   _____      | || |      __      | |  
# | |  |_   _|     | || |     /  \     | |  
# | |    | |       | || |    / /\ \    | |  
# | |    | |   _   | || |   / ____ \   | |  
# | |   _| |__/ |  | || | _/ /    \ \_ | |  
# | |  |________|  | || ||____|  |____|| |  
# | |              | || |              | |  
# | '--------------' || '--------------' |  
#  '----------------'  '----------------'   
#    _                       __  _            _____
#    (_)___  ____ ____  _____/ /_(_)___  ____ |__  /
#   / / __ \/ __ `/ _ \/ ___/ __/ / __ \/ __ \ /_ < 
#  / / / / / /_/ /  __(__  ) /_/ / /_/ / / / /__/ / 
# /_/_/ /_/\__, /\___/____/\__/_/\____/_/ /_/____/  
#         /____/                                    
#
# Wrapper script to run a provider end-to-end(ish). Terminates with JSON-L
# 
# sh ingest.sh pa /path/to/output /path/to/conf

# Get input 
provider=$1
baseOutDir=$2
conf=$3

# Future optional parameter to perform a clean build
clean_build="false"

# controlling progress
status=0

function timestamp() {
  end=$1
  start=$2
  i="$((end - start))"
  ((sec=i%60, i/=60, min=i%60, hrs=i/60))
  echo $(printf "%d:%02d:%02d" $hrs $min $sec)
}

# Example:
# ### Harvesting 
# ### Output: /Users/data/harvest
function printPhaseHeader() {
  echo "\n-----------------------------------------------------"
  echo "\t\t$1"
  echo "-----------------------------------------------------\n"
}

harvestDir="$baseOutDir/$provider/harvest/"
mappingDir="$baseOutDir/$provider/mapping/"
enrichDir="$baseOutDir/$provider/enrich/"
jsonlDir="$baseOutDir/$provider/jsonl/"

# Generate a clean build
if (( $clean_build == "true" )); then
  echo "Building project..."
  sbt clean compile package
fi

echo "\nStaring ingest..."
start=`date +%s`

###########
# Harvest #
###########
if [[ $status ==  0 ]]; then
  harvestStart=`date +%s`
  printPhaseHeader "Harvest"
  sbt --error "run-main dpla.ingestion3.HarvestEntry \
  --output=$harvestDir \
  --conf=$conf \
  --name=$provider" # > /dev/null 2>&1
  status=$?
  harvestEnd=`date +%s`
fi

############
#   Map    #
############
if [[ $status ==  0 ]]; then
  mapStart=`date +%s`
  printPhaseHeader "Mapping"
  sbt --error "run-main dpla.ingestion3.MappingEntry \
  --input=$harvestDir \
  --output=$mappingDir \
  --conf=$conf \
  --name=$provider"
  status=$?
  mapEnd=`date +%s`
fi

#############
#  Enrich   #
#############
if [[ $status == 0 ]]; then
  enrStart=`date +%s`
  printPhaseHeader "Enrichment"
  sbt --error "run-main dpla.ingestion3.EnrichEntry \
  --input=$mappingDir \
  --output=$enrichDir \
  --conf=$conf \
  --name=$provider" # > /dev/null 2>&1
  status=$?
  enrEnd=`date +%s`
fi

############
#  JSON-L  #
############
# if [[ $status == 0 ]]; then
#   sbt --error "run-main dpla.ingestion3.JsonlEntry $enrichDir $jsonlDir" # > /dev/null 2>&1
#   status=$?
# fi
end=`date +%s`

# Summary 
if [[ $status == 0 ]]; then
  echo "\n-----------------------------------------------------"
  echo "Ingest complete for $provider"
  echo "-----------------------------------------------------"
  echo "Harvest runtime: $(timestamp $harvestEnd $harvestStart)"
  echo "Mapping runtime: $(timestamp $mapEnd $mapStart)"
  echo "Enrich runtime:  $(timestamp $enrEnd $enrStart)"
  echo "------------------------"
  echo "Total runtime:   $(timestamp $end $start)"
  echo "-----------------------------------------------------"
  echo "Outputs"
  echo "--Harvest:    $harvestDir"
  echo "--Mapping:    $mappingDir"
  echo "--Enrichment: $enrichDir"
  echo "--JSON-L:     $jsonlDir"
  echo "-----------------------------------------------------"
else 
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "       Ingest failed"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
fi
