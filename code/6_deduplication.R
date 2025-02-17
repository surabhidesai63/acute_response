library(stringr) #for tslug
library(tidyverse)  
library(here) 
library(aws.s3)    

# Directory setup ----------------------------------------------------------------
# S3 Configuration
s3_bucket <- "acute-response-bucket"
log_file <- "logs/deduplication.log"
processed_dir <- "data/processed"

ensure_s3_directory <- function(bucket, dir) {
 # Get all object keys from the bucket
keys <- sapply(get_bucket(bucket = bucket), function(obj) obj$Key)
# Check if the directory exists
dir_exists <- any(startsWith(keys, dir))

# If the directory doesn't exist, create it by uploading a placeholder file
if (!dir_exists) {
  placeholder_file <- tempfile() # Create a temporary file
  writeLines("", placeholder_file) # Write an empty placeholder file
  put_object(file = placeholder_file, object = paste0(dir, "placeholder.txt"), bucket = bucket) # Upload to S3
  file.remove(placeholder_file) # Clean up the local file
    message(paste("S3 directory created:", dir))
 
   # After checking or creating the directory, remove the placeholder file
  delete_object(object = paste0(dir, "placeholder.txt"), bucket = bucket)
  message(paste("Removed placeholder file from S3:", dir))
} else {
    message(paste("S3 directory already exists:", dir))
}

}

# # Create directories in S3 /// commenting - runtime issues

ensure_s3_directory(s3_bucket, 'data/processed/')

# Logging Function ---------------------------------------------------------------
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

log_message("Starting deduplication script.")

# Helper function to find the latest and second latest files ---------------------
get_latest_files <- function(topic) {
  # List all files in the S3 directory
  files <- get_bucket(bucket = s3_bucket, prefix = processed_dir)


  # Return NULL if no files found
  if (is.null(files) || length(files) == 0) {
     message("No files found in S3 for prefix: ", processed_dir)
    return(list(latest = NULL, second_latest = NULL))
  }

  # Extract filenames
  files <- files %>% 
    purrr::map_chr("Key") %>% 
    .[str_detect(., paste0(topic, "_processed_.*\\.csv$"))]

  # Extract dates and remove NA values
  file_dates <- str_extract(files, "\\d{2}_\\d{2}") %>%
    na.omit() %>%
    as.Date(format = "%m_%d")

  # If no valid dates, return NULL
  if (length(file_dates) == 0) {
    return(list(latest = NULL, second_latest = NULL))
  }

  # Bind filenames and dates into a dataframe
  files_df <- tibble(
    file_path = files,
    file_date = file_dates,
    is_cumulative = str_detect(files, paste0(topic, "_processed_cumulative_"))
  )

  # Get the latest non-cumulative file
  latest_file <- files_df %>%
    filter(!is_cumulative) %>%
    arrange(desc(file_date)) %>%
    slice(1) %>%
    pull(file_path) %>%
    {if (length(.) == 0) NULL else .}

  # Get the latest cumulative file
second_latest_file <- files_df %>%
  arrange(desc(file_date)) %>%
  slice(2) %>%  # Get second latest file instead of just cumulative
  pull(file_path)

  return(list(latest = latest_file, second_latest = second_latest_file))
}

# Example usage
mpox_files <- get_latest_files("mpox")
flu_files <- get_latest_files("flu")
dengue_files <- get_latest_files("dengue")

log_message("Loaded file paths for datasets.")

# Load datasets ------------------------------------------------------------------
log_message("Loading cumulative and latest datasets.")

# Helper function to safely read from S3
safe_s3read_csv <- function(bucket, object) {
  if (!is.null(object) && length(object) > 0) {
    s3read_using(read_csv, bucket = bucket, object = object, show_col_types = FALSE)
  } else {
    warning(paste("⚠️ Skipping file read. No file found for:", object))
    return(NULL)  # Return NULL if file is missing
  }
}

# Read the second latest (cumulative) files from S3
cumulative_mpox <- safe_s3read_csv(s3_bucket, mpox_files$second_latest)
cumulative_flu <- safe_s3read_csv(s3_bucket, flu_files$second_latest)
cumulative_dengue <- safe_s3read_csv(s3_bucket, dengue_files$second_latest)

# Read the latest files from S3
latest_mpox <- safe_s3read_csv(s3_bucket, mpox_files$latest)
latest_flu <- safe_s3read_csv(s3_bucket, flu_files$latest)
latest_dengue <- safe_s3read_csv(s3_bucket, dengue_files$latest)

# Generate TSLUGs ----------------------------------------------------------------
log_message("Generating TSLUGs for datasets.")

generate_tslug <- function(text) {
  text <- tolower(text) %>% 
    gsub("[^a-zA-Z0-9\\s]", "", .) %>% 
    iconv(to = "ASCII//TRANSLIT") %>% 
    gsub("\\s+", "", .)
  return(text)
}

add_tslug <- function(dataset) {
  if (is.null(dataset)) {
    warning("⚠️ Skipping add_tslug: dataset is NULL.")
    return(NULL)
  }
  
  if (!"title" %in% colnames(dataset)) {
    warning("⚠️ Skipping add_tslug: 'title' column not found in dataset.")
    return(dataset)  # Return original dataset unchanged
  }

  # dataset %>%
  #   mutate(title_tslug = map_chr(title, generate_tslug, .default = NA_character_))  # Handle NA values safely
  safe_generate_tslug <- possibly(generate_tslug, otherwise = NA_character_)
  dataset %>%
    mutate(title_tslug = map_chr(title, safe_generate_tslug))
}

cumulative_mpox <- add_tslug(cumulative_mpox)
cumulative_flu <- add_tslug(cumulative_flu)
cumulative_dengue <- add_tslug(cumulative_dengue)

latest_mpox <- add_tslug(latest_mpox)
latest_flu <- add_tslug(latest_flu)
latest_dengue <- add_tslug(latest_dengue)

# Combine cumulative and latest datasets -----------------------------------------
log_message("Combining cumulative datasets with the latest batch.")

combined_mpox <- bind_rows(cumulative_mpox, latest_mpox)
combined_flu <- bind_rows(cumulative_flu, latest_flu)
combined_dengue <- bind_rows(cumulative_dengue, latest_dengue)

# Deduplicate datasets -----------------------------------------------------------
log_message("Deduplicating combined datasets.")

deduplicate_data <- function(dataset) {
  dataset %>%
    group_by(title_tslug) %>%
    arrange(desc(source == "Crossref"), desc(search_date)) %>%
    mutate(
      journal_title = coalesce(journal_title, first(journal_title[!is.na(journal_title)])),
      abstract = coalesce(abstract, first(abstract[!is.na(abstract)])),
      language = coalesce(language, first(language[!is.na(language)])),
      author = coalesce(author, first(author[!is.na(author)])),
      journal_date = coalesce(journal_date, first(journal_date[!is.na(journal_date)])),
      doi = coalesce(doi, first(doi[!is.na(doi)])),
      type = coalesce(type, first(type[!is.na(type)])),
      url = coalesce(url, first(url[!is.na(url)]))
    ) %>%
    slice(1) %>%
    ungroup()
}

deduplicated_mpox <- deduplicate_data(combined_mpox)
deduplicated_flu <- deduplicate_data(combined_flu)
deduplicated_dengue <- deduplicate_data(combined_dengue)

# Save deduplicated cumulative datasets ------------------------------------------
log_message("Saving updated cumulative datasets.")

# Save processed datasets to S3 -------------------------------------------------
save_to_s3 <- function(data, file_name) {
  # Create a temporary file to save the CSV data
  temp_file <- tempfile()
  
  # Write data to temporary file as CSV
  write_csv(data, temp_file)
  
  # Upload the CSV file to S3
  put_object(file = temp_file, object = file_name, bucket = s3_bucket)
  
  # Clean up the temporary file
  file.remove(temp_file)
  
  # Log message for confirmation
  log_message(paste("Processed file saved to S3:", file_name))
}
current_date <- format(as.Date(Sys.Date()), "%m_%d")  # Dynamic current date

save_to_s3(deduplicated_mpox, file.path(processed_dir, paste0("mpox_processed_cumulative_", current_date, ".csv")))
save_to_s3(deduplicated_flu, file.path(processed_dir, paste0("flu_processed_cumulative_", current_date, ".csv")))
save_to_s3(deduplicated_dengue, file.path(processed_dir, paste0("dengue_processed_cumulative_", current_date, ".csv")))

flush.console()
closeAllConnections()
log_message("Script #6 completed successfully. Exiting")
quit()
