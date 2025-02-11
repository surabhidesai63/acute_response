library(tidyverse)  
library(here)     
library(rcrossref)  
library(lubridate)  
library(xml2)
library(stringr)
library(aws.s3)

# Directory setup ----------------------------------------------------------------
# S3 Configuration
s3_bucket <- "acute-response-bucket"
log_file <- "logs/cleaning_log.log"
processed_dir <- "data/processed/"


ensure_s3_directory <- function(bucket, dir) {

dir_exists <- any(grepl(dir, get_bucket(bucket = bucket)$Key))

# If the directory doesn't exist, create it by uploading a placeholder file
if (!dir_exists) {
  placeholder_file <- tempfile() # Create a temporary file
  writeLines("", placeholder_file) # Write an empty placeholder file
  put_object(file = placeholder_file, object = paste0(dir, "placeholder.txt"), bucket = bucket) # Upload to S3
  file.remove(placeholder_file) # Clean up the local file
 message(paste("S3 directory created:", dir))
} else {
  message(paste("S3 directory already exists:", dir))
}

   # After checking or creating the directory, remove the placeholder file
  delete_object(object = paste0(dir, "placeholder.txt"), bucket = bucket)
  message(paste("Removed placeholder file from S3:", dir))
}

# # Create directories in S3 /// commenting - runtime issues

ensure_s3_directory(s3_bucket, processed_dir)

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

# Automatically find the most recent batch --------------------------------------
crossref_dir <-"data/1_crossref_responses"
pubmed_dir <- "data/3_extractions_from_pubmed_xml"

# Helper function to identify the latest files from S3
find_latest_files <- function(prefix, keyword) {
 # List files in the S3 bucket with the given prefix
  files <- get_bucket(bucket = s3_bucket, prefix = prefix)
  
  # Extract keys and timestamps
  file_keys <- sapply(files, function(x) x$Key)
  file_dates <- sapply(files, function(x) as.POSIXct(x$LastModified, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"))
  
  # Convert to data frame
  files_df <- data.frame(Key = file_keys, LastModified = file_dates, stringsAsFactors = FALSE)
  
  # Filter the list based on the keyword and ".csv" extension
  filtered_files <- files_df[grepl(paste0(keyword, ".*\\.csv$"), files_df$Key), ]
  
  # If no files match, return an empty character vector
  if (nrow(filtered_files) == 0) {
    return(character(0))
  }
  
  # Get the latest file based on LastModified
  latest_file <- filtered_files$Key[order(filtered_files$LastModified, decreasing = TRUE)][1]
  
  return(latest_file)
}


# Retrieve latest datasets
cf_mpox <- s3read_using(
  FUN = read_csv,  # Function to read CSV
  bucket = s3_bucket,  # Your S3 bucket name
  object = find_latest_files(crossref_dir, "mpox")  # Get latest file from S3
)
cf_flu <- s3read_using(
  FUN = read_csv,  # Function to read CSV
  bucket = s3_bucket,  # Your S3 bucket name
  object = find_latest_files(crossref_dir, "flu")  # Get latest file from S3
)
cf_dengue <- s3read_using(
  FUN = read_csv,  # Function to read CSV
  bucket = s3_bucket,  # Your S3 bucket name
  object = find_latest_files(crossref_dir, "dengue")  # Get latest file from S3
)


pm_mpox <- s3read_using(
  FUN = read_csv,  
  bucket = s3_bucket,  
  object = find_latest_files(pubmed_dir, "mpox")  
)

pm_flu <- s3read_using(
  FUN = read_csv,  
  bucket = s3_bucket,  
  object = find_latest_files(pubmed_dir, "flu")  
)

pm_dengue <- s3read_using(
  FUN = read_csv,  
  bucket = s3_bucket,  
  object = find_latest_files(pubmed_dir, "dengue")  
)

log_message("Loaded the latest batch of CrossRef and PubMed datasets.")
print("Loaded the latest batch of CrossRef and PubMed datasets.")
# Define a function to clean and process CrossRef data --------------------------
clean_crossref_data <- function(data) {
  log_message("cleaning crossref...")

  # Ensure necessary columns exist
  if (!"abstract" %in% colnames(data)) {
    data$abstract <- NA_character_
  }
  if (!"indexed" %in% colnames(data)) {
    data$indexed <- NA_character_
  }
  if (!"container.title" %in% colnames(data)) {
    data$container.title <- NA_character_
  }

  data %>%
    # Select only the available columns
    select(any_of(c("container.title", "title", "abstract", "language", "author", 
                    "indexed", "doi", "type", "url", "search_date"))) %>%
    
    rename(
      journal_title = container.title,
      journal_date = indexed
    ) %>%
    
    mutate(
      source = "Crossref",
      journal_date = as.character(journal_date),  # Convert to character
      
      # Clean abstract text while handling NA values safely
      abstract = ifelse(!is.na(abstract),
                        purrr::map_chr(abstract, function(x) {
                          tryCatch({
                            x %>%
                              gsub("<[^>]+>", " ", .) %>%  # Remove HTML/XML tags
                              gsub("^(Abstract|ABSTRACT)[[:space:][:punct:]]*", "", ., ignore.case = TRUE) %>%
                              stringr::str_squish()  # Remove extra spaces
                          }, error = function(e) NA_character_)  # Return NA if error occurs
                        }),
                        NA_character_)  # Ensure NA is returned if abstract is missing
    )
}


# Define a function to clean and process PubMed data ----------------------------
clean_pubmed_data <- function(data) {
  # Check if 'abstract' column exists
  if (!"abstract" %in% colnames(data)) {
    data$abstract <- NA_character_  # Create an empty column to prevent errors
  }

  data %>%
    # Select only available columns
    select(
      all_of(intersect(
        c("journal_title", "title", "abstract", "languages", "journal_date", "authors", "search_date"),
        colnames(data)
      ))
    ) %>%
    rename(
      author = authors,
      language = languages
    ) %>%
    mutate(
      source = "PubMed",
      journal_date = as.character(journal_date),  # Ensure journal_date is character
      type = NA,
      doi = NA,
      url = NA,
      abstract = ifelse(!is.na(abstract),
                        sapply(abstract, function(x) {
                          plain_text <- str_remove(x, "^(Abstract|ABSTRACT)\\s*")
                          str_squish(plain_text)  # Remove extra whitespace
                        }),
                        NA)  # Handle cases where the abstract is NA
    )
}



# Clean CrossRef datasets
cf_mpox <- clean_crossref_data(cf_mpox)
cf_flu <- clean_crossref_data(cf_flu)
cf_dengue <- clean_crossref_data(cf_dengue)

# Clean PubMed datasets
pm_mpox <- clean_pubmed_data(pm_mpox)
pm_flu <- clean_pubmed_data(pm_flu)
pm_dengue <- clean_pubmed_data(pm_dengue)

# Combine datasets after ensuring consistent types
combined_mpox <- bind_rows(cf_mpox, pm_mpox)
combined_flu <- bind_rows(cf_flu, pm_flu)
combined_dengue <- bind_rows(cf_dengue, pm_dengue)

# Function to standardize language codes ----------------------------------------
unify_language_codes <- function(language) {
  language_map <- list(
    "en" = "eng", "fr" = "fre", "de" = "deu", "es" = "spa", "pt" = "por",
    "hu" = "hun", "it" = "ita", "fi" = "fin", "zh" = "chi", "ms" = "msa",
    "ru-Latn" = "rus", "cy" = "cym", "hmn" = "hmn", "no" = "nor", "gl" = "glg"
  )
  return(language_map[[language]] %||% language)
}

# Standardize language codes ----------------------------------------------------
combined_mpox <- combined_mpox %>% mutate(language = sapply(language, unify_language_codes))
combined_flu <- combined_flu %>% mutate(language = sapply(language, unify_language_codes))
combined_dengue <- combined_dengue %>% mutate(language = sapply(language, unify_language_codes))

# Define the current date for file naming
current_date <- format(Sys.Date(), "%m_%d")

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
# Save processed datasets -------------------------------------------------------
current_date <- format(Sys.Date(), "%m_%d")
save_to_s3(combined_mpox, paste0(processed_dir, "mpox_processed_", current_date, ".csv"))
save_to_s3(combined_flu, paste0(processed_dir, "flu_processed_", current_date, ".csv"))
save_to_s3(combined_dengue, paste0(processed_dir, "dengue_processed_", current_date, ".csv"))

log_message("Processed datasets saved successfully.")
flush.console()
closeAllConnections()
log_message("Script #5 completed successfully. Exiting.")
quit()
