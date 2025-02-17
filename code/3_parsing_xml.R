library(tidyverse)
library(here)
library(XML)
library(xml2)
library(aws.s3)

# S3 Configuration
s3_bucket <- "acute-response-bucket"
log_file <- "logs/pubmed_parsing.log"         # Log file path in S3
xml_dir <- "data/2_pubmed_xml_responses/"    # Directory containing XML files in S3
output_dir <- "data/3_extractions_from_pubmed_xml"  # Directory to save CSV files in S3

# Ensure directories exist in S3 ------------------------------------------------
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
# ensure_s3_directory(s3_bucket, log_file)
ensure_s3_directory(s3_bucket, xml_dir)
ensure_s3_directory(s3_bucket, 'data/3_extractions_from_pubmed_xml/')

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

# Function to get the most recent `n` XML files from an S3 directory
get_recent_files <- function( xml_dir, n = 3) {
  # List all objects in the S3 directory
  s3_objects <- get_bucket(bucket = s3_bucket, prefix = xml_dir)
  
  # Extract file paths and dates
  file_dates <- tibble(
    file_path = s3_objects %>%
      sapply(function(x) x$Key) %>%
      Filter(function(path) str_detect(path, "\\.xml$"), .),  # Filter XML files
    end_date = s3_objects %>%
      sapply(function(x) x$Key) %>%
      Filter(function(path) str_detect(path, "\\.xml$"), .) %>%
      str_extract("_to_\\d{4}-\\d{2}-\\d{2}\\.xml") %>%
      str_remove_all("_to_|\\.xml") %>%
      as.Date(format = "%Y-%m-%d")
  ) %>%
    filter(!is.na(end_date)) %>%  # Remove files with invalid or missing dates
    arrange(desc(end_date))  # Sort by date in descending order
  
  # Select the most recent `n` files
  recent_files <- head(file_dates, n)
  
  log_message(paste("Found", nrow(recent_files), "most recent files for processing."))
  return(recent_files$file_path)
}

# Function to extract subject matter from XML files
extract_subject_matter_from_xml <- function(file_path,temp_file) {
  # # Ensure output directory exists
  # if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  # Extract search date from file name
  search_date <- file_path %>%
    str_extract("_to_\\d{4}-\\d{2}-\\d{2}\\.xml") %>%
    str_remove_all("_to_|\\.xml") %>%
    as.Date(format = "%Y-%m-%d")
  
  # Read the XML file
  xml_doc <- read_xml(get_object(object = file_path, bucket = s3_bucket))
  
  # Initialize lists to store extracted data
  list_pmids <- vector("character")
  list_titles <- vector("character")
  list_abstracts <- vector("character")
  list_languages <- vector("character")
  list_MESHs <- vector("character")
  list_keywords <- vector("character")
  list_journal_title <- vector("character")
  list_journal_issn <- vector("character")
  list_journal_volume <- vector("character")
  list_journal_issue <- vector("character")
  list_journal_date <- vector("character")
  list_dois <- vector("character")
  list_authors <- vector("character")
  
  # Extract <PubmedArticle> elements
  pubmed_articles <- xml_find_all(xml_doc, "/PubmedArticleSet/PubmedArticle")
  
  for (pubmed_article in pubmed_articles) {
    # Extract <MedlineCitation> section
    medline_node <- xml_find_first(pubmed_article, ".//MedlineCitation")
    pmid <- xml_text(xml_find_first(medline_node, ".//PMID"), trim = TRUE)
    
    # Extract article details
    node_article <- xml_find_first(medline_node, ".//Article")
    title <- xml_text(xml_find_first(node_article, ".//ArticleTitle"), trim = TRUE)
    abstracts <- xml_text(xml_find_all(node_article, ".//Abstract/AbstractText"), trim = TRUE)
    languages <- xml_text(xml_find_all(node_article, ".//Language"), trim = TRUE)
    
    # Extract DOI
    doi <- xml_text(xml_find_first(pubmed_article, ".//PubmedData/ArticleIdList/ArticleId[@IdType='doi']"), trim = TRUE)
    
    # Extract MESH terms
    node_MESHs <- xml_find_all(medline_node, ".//MeshHeadingList/MeshHeading")
    list_MESHs_inter <- vector("character")
    for (MESH in node_MESHs) {
      descriptor <- xml_text(xml_find_first(MESH, ".//DescriptorName"), trim = TRUE)
      qualifiers <- xml_text(xml_find_all(MESH, ".//QualifierName"), trim = TRUE)
      MESH_local <- paste(c(descriptor, qualifiers), collapse = " ")
      list_MESHs_inter <- c(list_MESHs_inter, MESH_local)
    }
    
    # Extract keywords
    keywords <- xml_text(xml_find_all(medline_node, ".//KeywordList/Keyword"), trim = TRUE)
    
    # Extract journal details
    node_journal <- xml_find_first(node_article, ".//Journal")
    journal_title <- xml_text(xml_find_first(node_journal, ".//Title"), trim = TRUE)
    journal_issn <- xml_text(xml_find_first(node_journal, ".//ISSN"), trim = TRUE)
    journal_volume <- xml_text(xml_find_first(node_journal, ".//Volume"), trim = TRUE)
    journal_issue <- xml_text(xml_find_first(node_journal, ".//Issue"), trim = TRUE)
    journal_date <- paste(
      xml_text(xml_find_first(node_journal, ".//PubDate/Year"), trim = TRUE),
      xml_text(xml_find_first(node_journal, ".//PubDate/Month"), trim = TRUE),
      xml_text(xml_find_first(node_journal, ".//PubDate/Day"), trim = TRUE),
      sep = "-"
    )
    
    # Extract authors
    node_authors <- xml_find_all(medline_node, ".//Author")
    authors_list <- vector("character")
    for (author in node_authors) {
      fore_name <- xml_text(xml_find_first(author, "ForeName"), trim = TRUE)
      last_name <- xml_text(xml_find_first(author, "LastName"), trim = TRUE)
      if (!is.na(fore_name) && !is.na(last_name)) {
        authors_list <- c(authors_list, paste(fore_name, last_name))
      }
    }
    authors <- paste(authors_list, collapse = ", ")
    
    # Append extracted data to lists
    list_pmids <- c(list_pmids, pmid)
    list_titles <- c(list_titles, title)
    list_abstracts <- c(list_abstracts, paste(abstracts, collapse = "--NEW SECTION--"))
    list_languages <- c(list_languages, paste(languages, collapse = ";"))
    list_dois <- c(list_dois, doi)
    list_MESHs <- c(list_MESHs, paste0(list_MESHs_inter, collapse = " "))
    list_keywords <- c(list_keywords, paste(keywords, collapse = "[AND]"))
    list_journal_title <- c(list_journal_title, journal_title)
    list_journal_issn <- c(list_journal_issn, journal_issn)
    list_journal_volume <- c(list_journal_volume, journal_volume)
    list_journal_issue <- c(list_journal_issue, journal_issue)
    list_journal_date <- c(list_journal_date, journal_date)
    list_authors <- c(list_authors, authors)
  }
  
  # Create a dataframe
  df_subject_matter <- data.frame(
    pmid = list_pmids,
    title = list_titles,
    abstract = list_abstracts,
    languages = list_languages,
    doi = list_dois,
    MESH_terms = list_MESHs,
    keywords = list_keywords,
    journal_title = list_journal_title,
    journal_issn = list_journal_issn,
    journal_volume = list_journal_volume,
    journal_issue = list_journal_issue,
    journal_date = list_journal_date,
    authors = list_authors,
    search_date = search_date
  )
  
  # Generate output file name
  output_file <- file.path(
    output_dir,
    paste0(
      "subject_matter_",
      case_when(
        str_detect(file_path, "dengue") ~ "dengue",
        str_detect(file_path, "mpox|monkeypox|monkey pox|mpxv") ~ "mpox",
        str_detect(file_path, "influenza|flu") ~ "flu",
        TRUE ~ "unknown"
      ),
      "_",
      format(search_date, "%Y_%m_%d"),
      ".csv"
    )
  )
  
  temp_file1 <- tempfile(fileext = ".csv")  
  # Save the dataframe to the temporary file
  write.csv(df_subject_matter, temp_file1, row.names = FALSE)  
  # Upload the file to S3
  put_object(file = temp_file1, object = output_file, bucket = s3_bucket)  
  # Clean up by removing the temporary file
  file.remove(temp_file1)
  log_message(paste("Parsed and saved data to:", output_file))
}
# Main Script -------------------------------------------------------------------
log_message("Starting XML parsing script...")

# Get the most recent files
tryCatch(
  {
    recent_files <- get_recent_files(xml_dir, n = 3)
    if (length(recent_files) == 0) {
      log_message("No recent XML files found. Exiting script.")
      quit(save = "no", status = 0)  # Exit gracefully if no files to process
    }
  },
  error = function(e) {
    log_message(paste("Error in fetching recent files:", e$message))
    quit(save = "no", status = 1)  # Exit with error status
  }
)

# Process each file
for (file_path in recent_files) {
  tryCatch(
    {
      log_message(paste("Processing file:", file_path))
      # temp_file <- tempfile(fileext = ".xml")
      # save_object(object = file_path, bucket = s3_bucket, file = temp_file)
     
      extract_subject_matter_from_xml(file_path,temp_file)
      log_message(paste("Successfully processed file:", file_path))
    },
    error = function(e) {
      log_message(paste("Error processing file:", file_path, " - ", e$message))
    }
  )
}

flush.console()
closeAllConnections()
log_message("Script #3 completed successfully. Exiting.")
quit()
