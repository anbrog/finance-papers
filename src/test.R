install.packages("remotes")
remotes::install_github("ropensci/openalexR")

options(openalexR.mailto = "andreasbrogger@gmail.com")

library(openalexR)
library(dplyr)
library(ggplot2)

works_from_dois <- oa_fetch(
  entity = "works",
  doi = c("10.1016/j.joi.2017.08.007", "https://doi.org/10.1007/s11192-013-1221-3"),
  verbose = TRUE
)
#> Requesting url: https://api.openalex.org/works?filter=doi%3A10.1016%2Fj.joi.2017.08.007%7Chttps%3A%2F%2Fdoi.org%2F10.1007%2Fs11192-013-1221-3
#> Getting 1 page of results with a total of 2 records...
# str(works_from_dois, max.level = 2)
# head(works_from_dois)
# show_works(works_from_dois)

works_from_dois |>
  show_works() |>
  knitr::kable()

# cool example -----
works_search <- oa_fetch(
  entity = "works",
  title.search = c("bibliometric analysis", "science mapping"),
  cited_by_count = ">50",
  from_publication_date = "2020-01-01",
  to_publication_date = "2021-12-31",
  options = list(sort = "cited_by_count:desc"),
  verbose = TRUE
)
#> Requesting url: https://api.openalex.org/works?filter=title.search%3Abibliometric%20analysis%7Cscience%20mapping%2Ccited_by_count%3A%3E50%2Cfrom_publication_date%3A2020-01-01%2Cto_publication_date%3A2021-12-31&sort=cited_by_count%3Adesc
#> Getting 3 pages of results with a total of 455 records...
works_search |>
  show_works() |>
  knitr::kable()