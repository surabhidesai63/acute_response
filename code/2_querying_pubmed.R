library(tidyverse)
library(here)
library(rentrez)
library(XML)
library(aws.s3) 


# AWS S3 configuration --------------------------------------------------------
s3_bucket <- "acute-response-bucket"
data_dir <- "data/"
log_dir <- "logs/"
date_file <- paste0(data_dir, "last_pubmed_extract_date.txt")
log_file<- paste0(log_dir, "pubmed_query.log")


# Set API key -------------------------------------------------------------------
api_key <- "4e7696f07a04e00b907d0f55a80aaa100808"
set_entrez_key(api_key)

# Logging function -------------------------------------------------------------
log_message <- function(message_text) {
   # Create a temporary log file for storing logs during Lambda execution
  temp_log_file <- tempfile(fileext = ".log")
  
  # Generate timestamped log message
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- paste0("[", timestamp, "] ", message_text, "\n")
  
  # Print log message to console (useful for debugging in Lambda)
  cat(message)
  
  # Write log message to the temporary log file
  write(message, file = temp_log_file, append = TRUE)
  
  # At the end of execution, upload the log file to S3
  tryCatch({
    put_object(
      file = temp_log_file, 
      object = file.path(log_file), 
      bucket = s3_bucket
    )
    cat("[", timestamp, "] Log file successfully uploaded to S3.\n")
  }, error = function(e) {
    cat("[", timestamp, "] Failed to upload log file to S3: ", e$message, "\n")
  })
}

# Retrieve last extract date ---------------------------------------------------
if (object_exists(object = date_file, bucket = s3_bucket)) {
  # Download the file from S3
  temp_date_file <- tempfile()
  save_object(object = date_file, bucket = s3_bucket, file = temp_date_file)
  
  # Read the date from the downloaded file
  date_from <- readLines(temp_date_file, warn = FALSE) %>% as.Date()
  log_message(paste("Last extraction date retrieved:", date_from))
  
  # Clean up the temporary file
  unlink(temp_date_file)
} else {
  # Set a default date if the file doesn't exist
  date_from <- as.Date("2024-12-09")
  log_message("No previous extraction date found. Defaulting to 2024-12-09.")
  
  # Write the default date to a temporary file
  temp_date_file <- tempfile()
  writeLines(as.character(date_from), temp_date_file)
  
  # Upload the temporary file to S3
  put_object(file = temp_date_file, object = date_file, bucket = s3_bucket)
  log_message(paste("Created date file with default date in S3:", date_from))
  
  # Clean up the temporary file
  unlink(temp_date_file)
}

date_to <- Sys.Date()  # Current date
log_message(paste("Extracting records from:", date_from, "to:", date_to))

# Define query terms -----------------------------------------------------------
query_terms <- c(
  '("mpox"[Title/Abstract] OR "monkeypox"[Title/Abstract] OR "monkey pox"[Title/Abstract] OR "mpxv"[Title/Abstract])',
  '("influenza"[Title/Abstract] OR "flu"[Title/Abstract])',
  '("dengue"[Title/Abstract])'
)

# Helper function to format date ranges ----------------------------------------
format_custom_date_range <- function(start_date, end_date) {
  paste0('("', start_date, '"[Date - Publication] : "', end_date, '"[Date - Publication])')
}

# Function to fetch PubMed records ---------------------------------------------
fetch_pubmed_records <- function(query, start_date, end_date) {
  # Format the date range
  query_date_range <- format_custom_date_range(start_date, end_date)
  # Combine query terms and date range
  full_query <- paste0(query, ' AND ', query_date_range)
  
  log_message(paste("Querying PubMed for:", full_query))
  
  # Perform PubMed search
  query_search <- tryCatch({
    entrez_search(db = "pubmed", term = full_query, use_history = TRUE)
  }, error = function(e) {
    log_message(paste("Error querying PubMed for:", query, "Error:", e$message))
    return(NULL)
  })
  
  # Handle case where no results are returned
  if (is.null(query_search) || query_search$count == 0) {
    log_message(paste("No results found for query:", query))
    return(NULL)
  }
  
  log_message(paste("Found", query_search$count, "results for query:", query))
  
  # Fetch records
  records <- tryCatch({
    entrez_fetch(
      db = "pubmed",
      web_history = query_search$web_history,
      retmax = 20000,  # Adjust as needed
      rettype = "xml",
      parsed = TRUE
    )
  }, error = function(e) {
    log_message(paste("Error fetching records for:", query, "Error:", e$message))
    return(NULL)
  })
  
  if (is.null(records)) return(NULL)
  
output_dir <- "data/2_pubmed_xml_responses/"
   
# Get all object keys from the bucket
keys <- sapply(get_bucket(bucket = s3_bucket), function(obj) obj$Key)

# Check if the directory exists
output_dir_exists <- any(startsWith(keys, output_dir))

# If the directory doesn't exist, create it by uploading a placeholder file
if (!output_dir_exists) {
  placeholder_file <- tempfile() # Create a temporary file
  writeLines("", placeholder_file) # Write an empty placeholder file
  put_object(file = placeholder_file, object = paste0(output_dir, "placeholder.txt"), bucket = s3_bucket) # Upload to S3
  file.remove(placeholder_file) # Clean up the local file
  message(paste("S3 directory created:", output_dir))
} else {
  message(paste("S3 directory already exists:", output_dir))
}
  # After checking or creating the directory, remove the placeholder file
  delete_object(object = paste0(output_dir, "placeholder.txt"), bucket = s3_bucket)
  message(paste("Removed placeholder file from S3:", output_dir))  
   
# Map the query to a clean name
query_clean <- case_when(
  str_detect(query, "mpox|monkeypox|monkey pox|mpxv") ~ "mpox",
  str_detect(query, "influenza|flu") ~ "flu",
  str_detect(query, "dengue") ~ "dengue",
  TRUE ~ "query"
)

# Format the date strings for the file name
formatted_date_from <- format(as.Date(start_date), "%Y-%m-%d")
formatted_date_to <- format(as.Date(end_date), "%Y-%m-%d")

# Generate the file name for S3
file_name <- paste0(
  output_dir,
  "query_results_", query_clean, "_", formatted_date_from, "_to_", formatted_date_to, ".xml"
)

# Save XML content and upload to S3
tryCatch({
  # Convert the XML content to a string
  xml_string <- XML::saveXML(records)
  
  # Upload the XML string to S3
  put_object(file = charToRaw(xml_string), object = file_name, bucket = s3_bucket)
  
  log_message(paste("Saved records to S3:", file_name))
}, error = function(e) {
  log_message(paste("Error saving records to S3:", e$message))
})
}

# Loop through query terms -----------------------------------------------------
for (query in query_terms) {
  fetch_pubmed_records(query, date_from, date_to)
}

# Update last extraction date --------------------------------------------------
tryCatch({
  temp_date_file <- tempfile()
  writeLines(as.character(date_to), temp_date_file)
  put_object(file = temp_date_file, object = date_file, bucket = s3_bucket)
  log_message(paste("Updated last extraction date in S3 to:", date_to))
  
  # Clean up the temporary file
  unlink(temp_date_file)
}, error = function(e) {
  log_message(paste("Error updating last extraction date in S3:", e$message))
})

flush.console()
closeAllConnections()
log_message("Script #2 completed successfully. Exiting.")
quit()
