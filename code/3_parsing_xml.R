# Load necessary packages --------------------------------------------------------
if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("here", quietly = TRUE)) install.packages("here")
if (!requireNamespace("XML", quietly = TRUE)) install.packages("XML")
if (!requireNamespace("xml2", quietly = TRUE)) install.packages("xml2")

library(tidyverse)
library(here)
library(XML)
library(xml2)

# Directory setup ----------------------------------------------------------------
here::i_am("code/3_parsing_xml.R")
log_file <- here("logs/pubmed_parsing.log")  # Log file path
xml_dir <- here("data/2_pubmed_xml_responses")  # Directory containing XML files
output_dir <- here("data/3_extractions_from_pubmed_xml")  # Directory to save CSV files

# Ensure directories exist
if (!dir.exists(dirname(log_file))) dir.create(dirname(log_file), recursive = TRUE)
if (!dir.exists(xml_dir)) dir.create(xml_dir, recursive = TRUE)
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Logging function ---------------------------------------------------------------
log_message <- function(message_text) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- paste0("[", timestamp, "] ", message_text, "\n")
  cat(message)  # Print to console
  write(message, file = log_file, append = TRUE)
}

# Get the most recent 3 files ---------------------------------------------------
get_recent_files <- function(xml_dir, n = 3) {
  # List all XML files in the directory
  xml_files <- list.files(xml_dir, pattern = "\\.xml$", full.names = TRUE)
  
  # Extract the end dates from the file names
  file_dates <- tibble(
    file_path = xml_files,
    end_date = xml_files %>%
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

# SUBJECT MATTER EXTRACTION function --------------------------------------------
extract_subject_matter_from_xml <- function(file_path) {
  # Extract search date from file name
  search_date <- file_path %>%
    str_extract("_to_\\d{4}-\\d{2}-\\d{2}\\.xml") %>%
    str_remove_all("_to_|\\.xml") %>%
    as.Date(format = "%Y-%m-%d")
  
  xml_doc <- read_xml(file_path)
  
  # Create empty lists to store extracted info
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
  
  # Extracting the <PubmedArticle> elements
  pubmed_articles <- xml_find_all(xml_doc, "/PubmedArticleSet/PubmedArticle")
  
  for (pubmed_article in pubmed_articles) {
    # Extract <MedlineCitation> section
    medline_node <- xml_find_first(pubmed_article, ".//MedlineCitation")
    pmid <- xml_text(xml_find_first(medline_node, ".//PMID"))
    
    # In context <Article>
    node_article <- xml_find_first(medline_node, ".//Article")
    title <- xml_text(xml_find_first(node_article, ".//ArticleTitle"))
    abstracts <- xml_text(xml_find_all(node_article, ".//Abstract/AbstractText"))
    languages <- xml_text(xml_find_all(node_article, ".//Language"))
    
    # DOI extraction from PubmedData
    doi <- xml_text(xml_find_first(pubmed_article, ".//PubmedData/ArticleIdList/ArticleId[@IdType='doi']"))
    
    # In context <MedlineCitation>, looking for MESH terms
    node_MESHs <- xml_find_all(medline_node, ".//MeshHeadingList/MeshHeading")
    list_MESHs_inter <- vector("character")
    for (MESH in node_MESHs) {
      descriptor <- xml_text(xml_find_first(MESH, ".//DescriptorName"))
      qualifiers <- xml_text(xml_find_all(MESH, ".//QualifierName"))
      MESH_local <- paste(c(descriptor, qualifiers), collapse = " ")
      list_MESHs_inter <- c(list_MESHs_inter, MESH_local)
    }
    
    # In context <MedlineCitation>
    keywords <- xml_text(xml_find_all(medline_node, ".//KeywordList/Keyword"))
    
    # In context <Journal>
    node_journal <- xml_find_first(node_article, ".//Journal")
    journal_title <- xml_text(xml_find_first(node_journal, ".//Title"))
    journal_issn <- xml_text(xml_find_first(node_journal, ".//ISSN"))
    journal_volume <- xml_text(xml_find_first(node_journal, ".//Volume"))
    journal_issue <- xml_text(xml_find_first(node_journal, ".//Issue"))
    journal_date <- paste(xml_text(xml_find_first(node_journal, ".//PubDate/Year")),
                          xml_text(xml_find_first(node_journal, ".//PubDate/Month")),
                          xml_text(xml_find_first(node_journal, ".//PubDate/Day")), sep = "-")
    
    # Author extraction
    node_authors <- xml_find_all(medline_node, ".//Author")
    authors_list <- vector("character")
    for (author in node_authors) {
      fore_name <- xml_text(xml_find_first(author, "ForeName"))
      last_name <- xml_text(xml_find_first(author, "LastName"))
      if (!is.na(fore_name) && !is.na(last_name)) {
        authors_list <- c(authors_list, paste(fore_name, last_name))
      }
    }
    authors <- paste(authors_list, collapse = ", ")
    
    # Append the data to our lists
    list_pmids <- c(list_pmids, pmid)
    list_titles <- c(list_titles, title)
    list_abstracts <- c(list_abstracts, paste(abstracts, collapse = "--NEW SECTION--"))
    list_languages <- c(list_languages, paste(languages, collapse = ";"))
    list_dois <- c(list_dois, doi)
    list_MESHs <- c(list_MESHs, paste0(list_MESHs_inter, collapse = ""))
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
  
  # Update file naming logic
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
      format(search_date, "%m_%d"),
      ".csv"
    )
  )
  
  # Write the dataframe to a CSV file
  write_csv(df_subject_matter, output_file)
  log_message(paste("Parsed and saved to:", output_file))
}

# Main script -------------------------------------------------------------------
log_message("Starting XML parsing script...")
recent_files <- get_recent_files(xml_dir, n = 3)

for (file_path in recent_files) {
  log_message(paste("Processing file:", file_path))
  extract_subject_matter_from_xml(file_path)
}

log_message("XML parsing script completed successfully.")
