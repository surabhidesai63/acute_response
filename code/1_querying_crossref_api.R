# Load necessary packages --------------------------------------------------------
library(tidyverse)  
library(here)     
library(rcrossref)  
library(lubridate)  
library(writexl)
library(aws.s3) 


# AWS S3 configuration ---------------------------------------------------------
###### Replace with your S3 bucket details -added by surabhi
s3_bucket <- "acute-response-bucket"
data_dir <- "data/"
log_dir <- "logs/"
date_file <- paste0(data_dir, "last_crossref_extract_date.txt")
log_file<- paste0(log_dir, "crossref_query.log")


log_message <- function(message) {

  # Append the log message to the log file in S3
    tryCatch({
      # Download existing log file (if it exists)
      temp_log_file <- tempfile()
      if (object_exists(bucket = s3_bucket, object = log_file)) {
        save_object(object = log_file, bucket = s3_bucket, file = temp_log_file)
      }
      
      # Append the new log message
      cat(paste0(Sys.time(), " ", message, "\n"), file = temp_log_file, append = TRUE)
      
      # Upload the updated log file back to S3
      put_object(file = temp_log_file, object = log_file, bucket = s3_bucket)
      unlink(temp_log_file) # Clean up temporary file
    }, error = function(e) {
      message(paste("Error writing log to S3:", e$message))
    })
}

##### Retrieve last extract date - added by surabhi
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
################################################################
date_to <- Sys.Date()  # Current date

# Define query terms -------------------------------------------------------------
query_terms <- c("(mpox OR monkeypox OR \"monkey pox\" OR mpxv)", "(influenza OR flu)", "dengue")

# Log the start of the script ----------------------------------------------------
log_message("Starting CrossRef queries...")
log_message(paste("Querying from:", date_from, "to:", date_to))

# Loop through each query term ---------------------------------------------------
for (query in query_terms) {
  log_message(paste("Processing query term:", query))
  
  # Perform search with query and date filter
  log_message("Querying CrossRef API...")
  results <- tryCatch({
    cr_works(query = query, filter = c(from_pub_date = as.character(date_from), until_pub_date = as.character(date_to)), limit = 1000)
  }, error = function(e) {
    log_message(paste("Error querying CrossRef API for query:", query, "Error:", e$message))
    return(NULL)  # Skip to the next query term if an error occurs
  })
  
  # Refine results
  if (is.null(results)) next
  results_df <- results$data
  if (is.null(results_df) || nrow(results_df) == 0) {
    log_message(paste("No results returned for query:", query))
    next  # Skip to the next query term
  }
  
  # Add a timestamp to indicate when the data was fetched
  search_date <- format(Sys.time(), "%Y-%m-%d")
  
  # Clean and format results
  log_message("Cleaning and formatting results...")
  format_authors <- function(authors_tibble) {
    if (is.data.frame(authors_tibble) && nrow(authors_tibble) > 0 &&
        all(c("given", "family") %in% colnames(authors_tibble))) {
      authors_list <- authors_tibble %>%
        mutate(author_name = str_c(family, ", ", given)) %>%
        pull(author_name)
      str_c(authors_list, collapse = ", ")
    } else {
      NA
    }
  }
  
  cleaned_results_df <- results_df %>%
    mutate(search_date = search_date, author = sapply(author, format_authors))
  


  ########################### code added by surabhi
output_dir <- "data/1_crossref_responses/"  # S3 directory (prefix)

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
  
  # Clean query term for file naming
  query_clean <- case_when(
    str_detect(query, "mpox|monkeypox|monkey pox|mpxv") ~ "mpox",
    str_detect(query, "influenza|flu") ~ "flu",
    str_detect(query, "dengue") ~ "dengue",
    TRUE ~ "query"
  )
  
  # Format date strings for file name
  formatted_date_from <- format(as.Date(date_from), "%m_%d")
  formatted_date_to <- format(as.Date(date_to), "%m_%d")
  


  # Define the output file path in S3
output_file <- paste0(
  output_dir, 
  "crossref_", query_clean, "_", formatted_date_from, "_to_", formatted_date_to, ".csv"
)

# Write results to CSV
log_message(paste("Writing results to S3 path:", output_file))
tryCatch({
  # Save the data frame to a temporary file
  temp_file <- tempfile(fileext = ".csv")
  write_csv(cleaned_results_df, file = temp_file)
  
  # Upload the temporary file to S3
  put_object(file = temp_file, object = output_file, bucket = s3_bucket)
  log_message("File written successfully to S3.")
  
  # Clean up the temporary file
  unlink(temp_file)
  
}, error = function(e) {
  log_message(paste("Error writing file for query:", query, "Error:", e$message))
})

# Update the last extraction date in S3
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
}

flush.console()
closeAllConnections()
log_message("Script #1 completed successfully. Exiting.")
quit()
