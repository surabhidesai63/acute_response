# Load required libraries
if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("here", quietly = TRUE)) install.packages("here")
if (!requireNamespace("rentrez", quietly = TRUE)) install.packages("rentrez")
if (!requireNamespace("XML", quietly = TRUE)) install.packages("XML")

library(tidyverse)
library(here)
library(rentrez)
library(XML)

# Directory setup --------------------------------------------------------------# xml_dir <> output_dir - optimize
here::i_am("code/2_querying_pubmed.R")
log_file <- here("logs/pubmed_query.log")  # Log file path
date_file <- here("data/last_pubmed_extract_date.txt")  # File to store the last extract date

# Ensure directories exist
if (!dir.exists(dirname(log_file))) dir.create(dirname(log_file), recursive = TRUE)
if (!dir.exists(dirname(date_file))) dir.create(dirname(date_file), recursive = TRUE)

# Set API key -------------------------------------------------------------------
api_key <- "4e7696f07a04e00b907d0f55a80aaa100808"
set_entrez_key(api_key)

# Logging function -------------------------------------------------------------
log_message <- function(message_text) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- paste0("[", timestamp, "] ", message_text, "\n")
  cat(message)  # Print to console
  write(message, file = log_file, append = TRUE)
}

# Retrieve last extract date ---------------------------------------------------
if (file.exists(date_file)) {
  date_from <- readLines(date_file, warn = FALSE) %>% as.Date()
  if (is.na(date_from)) {
    date_from <- as.Date("2024-12-09")  # Default to last known extraction date
    log_message("No valid date found in file. Defaulting to 2024-12-09.")
  } else {
    log_message(paste("Last extraction date retrieved:", date_from))
  }
} else {
  date_from <- as.Date("2024-12-09")  # Default to last known extraction date
  log_message("No previous extraction date found. Defaulting to 2024-12-09.")
  writeLines(as.character(date_from), date_file)  # Create the date file with the default date
  log_message(paste("Created date file with default date:", date_from))
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
  
  # Save records to an XML file
  output_dir <- here("data/2_pubmed_xml_responses")
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
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
  
  file_name <- file.path(
    output_dir,
    paste0("query_results_", query_clean, "_", formatted_date_from, "_to_", formatted_date_to, ".xml")
  )
  
  XML::saveXML(records, file = file_name)
  log_message(paste("Saved records to:", file_name))
}

# Loop through query terms -----------------------------------------------------
for (query in query_terms) {
  fetch_pubmed_records(query, date_from, date_to)
}

# Update last extraction date --------------------------------------------------
writeLines(as.character(date_to), date_file)
log_message(paste("Updated last extraction date to:", date_to))

log_message("Script completed successfully.")
