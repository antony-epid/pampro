#!/bin/bash

# Check if one argument is provided
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 dirname monitor_type"
  exit 1
fi

PROJECT_DIR="$1"
echo "The project directory is $PROJECT_DIR"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "Directory $PROJECT_DIR does not yet exist."
  exit 1
fi

FILE_TYPE="$2"
#make it lower case
FILE_TYPE="${FILE_TYPE,,}"
echo $FILE_TYPE

if [[ "$FILE_TYPE" == 'axiv' ]]; then
   typef="Axivity"
   ext=".cwa"
elif [[ "$FILE_TYPE" == 'gene' ]]; then
   typef="GeneActiv"
   ext=".bin"
else
  echo "Unknown monitor type. Write 'axiv' for Axivity or 'gene' for GeneActiv as the second argument."
  exit 1
fi


DATA_DIR="$PROJECT_DIR/_data"
LOG_DIR="$PROJECT_DIR/_logs"
ANOM_DIR="$PROJECT_DIR/_anomalies"
HDF5_DIR="$PROJECT_DIR/_hdf5"
CONF_DIR="$PROJECT_DIR/_config"
RES_DIR="$PROJECT_DIR/_results"
PLOT_DIR="$PROJECT_DIR/_plots"
STIL_DIR="$PROJECT_DIR/_stillbouts"
REL_DIR="$PROJECT_DIR/_releases"
DOC_DIR="$PROJECT_DIR/_documents"
AN_DIR="$PROJECT_DIR/_analysis"

# Define a list of items
dirs=($DATA_DIR $LOG_DIR $ANOM_DIR $HDF5_DIR $CONF_DIR $PLOT_DIR $STIL_DIR $RES_DIR $REL_DIR $DOC_DIR $AN_DIR)

# Loop through each item
for dir in "${dirs[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "Directory $dir_path does not yet exist."
    # Optionally, create the directory
    mkdir -p "$dir"
    #echo "Directory $dir has been created."
  else
    echo "Directory $dir already exists."
  fi
done


# Initialize arrays to store headers and their corresponding values
headers=()
values=()

# Function to add header and its corresponding value
add_field() {
  local header=$1
  local value=$2
  headers+=("$header")
  values+=("$value")
}


# # Function to add header and its corresponding value
# add_field() {
#   local header=$1
#   local value=$2
#   headers+=("$header")
#   # Escape double quotes in the value
#   value="${value//\"/\\\"}"  # Replace " with \"
#   values+=("$value")
# }

# Add header-value pairs dynamically
add_field "stillbouts_folder" "$STIL_DIR"
add_field "config_folder" "$CONF_DIR"
add_field "processing_epoch" 5 
add_field "target_frequency" 100
add_field "monitor_type" "$typef"
add_field "raw_file_extension" "$ext"
add_field "project_root" "$PROJECT_DIR"
add_field "anomalies_folder" "$ANOM_DIR"
add_field "noise_cutoff_mg" 13
add_field "logs_folder" "$LOG_DIR"
add_field "plots_folder" "$PLOT_DIR"
add_field "temperature_calibration" "YES" 
add_field "results_folder" "$RES_DIR"
add_field "raw_data_folder" "$DATA_DIR"
add_field "hdf5_folder" "$HDF5_DIR"
add_field "submission_id" 0


# Join arrays to form CSV header and row
header_line=$(IFS=,; echo "${headers[*]}")
value_line=$(IFS=,; echo "${values[*]}")

CSV_FILE="$CONF_DIR/general_settings.csv"

# Create the CSV file and write the header and the single data row
echo "$header_line" > "$CSV_FILE"
echo "$value_line" >> "$CSV_FILE"

echo "CSV file $CSV_FILE has been created with the following content:"
cat "$CSV_FILE"

# The following are for generating standard analysis setting
# Function to generate the JSON-like pattern
generate_cutpoints() {
  local cutpoints="["
  local start_values=(0 1 2 3 4 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 105 110 115 120 125 130 135 140 145 150 160 170 180 190 200 210 220 230 240 250 260 270 280 290 300 400 500 600 700 800 900 1000 2000 3000 4000)
  for start in "${start_values[@]}"; do
    cutpoints+="{\"start\":$start,\"end\":99999},"
  done
  cutpoints="${cutpoints%,}]"  # Remove the trailing comma and close the array
  echo "$cutpoints"
}

# Generate the cutpoints pattern
cutpoints=$(generate_cutpoints)

#create standard analysis setting file 
#Add more header-value pairs to to the existing header and value variables

add_field "battery_plot"  0
add_field "pitch_plot"  1
add_field "battery" 0
add_field "cutpoints" '"[{""start"":0,""end"":99999},{""start"":1,""end"":99999},{""start"":2,""end"":99999},{""start"":3,""end"":99999},{""start"":4,""end"":99999},{""start"":5,""end"":99999},{""start"":10,""end"":99999},{""start"":15,""end"":99999},{""start"":20,""end"":99999},{""start"":25,""end"":99999},{""start"":30,""end"":99999},{""start"":35,""end"":99999},{""start"":40,""end"":99999},{""start"":45,""end"":99999},{""start"":50,""end"":99999},{""start"":55,""end"":99999},{""start"":60,""end"":99999},{""start"":65,""end"":99999},{""start"":70,""end"":99999},{""start"":75,""end"":99999},{""start"":80,""end"":99999},{""start"":85,""end"":99999},{""start"":90,""end"":99999},{""start"":95,""end"":99999},{""start"":100,""end"":99999},{""start"":105,""end"":99999},{""start"":110,""end"":99999},{""start"":115,""end"":99999},{""start"":120,""end"":99999},{""start"":125,""end"":99999},{""start"":130,""end"":99999},{""start"":135,""end"":99999},{""start"":140,""end"":99999},{""start"":145,""end"":99999},{""start"":150,""end"":99999},{""start"":160,""end"":99999},{""start"":170,""end"":99999},{""start"":180,""end"":99999},{""start"":190,""end"":99999},{""start"":200,""end"":99999},{""start"":210,""end"":99999},{""start"":220,""end"":99999},{""start"":230,""end"":99999},{""start"":240,""end"":99999},{""start"":250,""end"":99999},{""start"":260,""end"":99999},{""start"":270,""end"":99999},{""start"":280,""end"":99999},{""start"":290,""end"":99999},{""start"":300,""end"":99999},{""start"":400,""end"":99999},{""start"":500,""end"":99999},{""start"":600,""end"":99999},{""start"":700,""end"":99999},{""start"":800,""end"":99999},{""start"":900,""end"":99999},{""start"":1000,""end"":99999},{""start"":2000,""end"":99999},{""start"":3000,""end"":99999},{""start"":4000,""end"":99999}]"' 
add_field "days_of_data" 0
add_field "epochs_plot" '"[{""name"":""1m"",""plot"":1},{""name"":""1h"",""plot"":0}]"'
add_field "roll_plot" 1
add_field "temperature_plot" 0
add_field "angles" '"[{""start"":-90,""end"":-85},{""start"":-85,""end"":-80},{""start"":-80,""end"":-75},{""start"":-75,""end"":-70},{""start"":-70,""end"":-65},{""start"":-65,""end"":-60},{""start"":-60,""end"":-55},{""start"":-55,""end"":-50},{""start"":-50,""end"":-45},{""start"":-45,""end"":-40},{""start"":-40,""end"":-35},{""start"":-35,""end"":-30},{""start"":-30,""end"":-25},{""start"":-25,""end"":-20},{""start"":-20,""end"":-15},{""start"":-15,""end"":-10},{""start"":-10,""end"":-5},{""start"":-5,""end"":0},{""start"":0,""end"":5},{""start"":5,""end"":10},{""start"":10,""end"":15},{""start"":15,""end"":20},{""start"":20,""end"":25},{""start"":25,""end"":30},{""start"":30,""end"":35},{""start"":35,""end"":40},{""start"":40,""end"":45},{""start"":45,""end"":50},{""start"":50,""end"":55},{""start"":55,""end"":60},{""start"":60,""end"":65},{""start"":65,""end"":70},{""start"":70,""end"":75},{""start"":75,""end"":80},{""start"":80,""end"":85},{""start"":85,""end"":90}]"'
add_field "temperature" 0
add_field "epochs"  '"[{""increment"":1,""unit"":""minute(s)""},{""increment"":1,""unit"":""hour(s)""}]"'
add_field "collapse_data" "YES"
add_field "whole_file"  "YES"
add_field "enmo" 1
add_field "enmo_plot" 1
add_field "hpfvm_plot" 1
add_field "hpfvm" 1
add_field "pitch" 1
add_field "roll" 1


# Join arrays to form CSV header and row
header_line=$(IFS=,; echo "${headers[*]}")
value_line=$(IFS=,; echo "${values[*]}")

CSV_FILE="$CONF_DIR/standardanalysis_settings.csv"

# Create the CSV file and write the header and the single data row
echo "$header_line" > "$CSV_FILE"
echo "$value_line" >> "$CSV_FILE"

echo "CSV file $CSV_FILE has been created with the following content:"
cat "$CSV_FILE"

