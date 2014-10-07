(ns dwd.files.file-system
  "filesystem-based operations"
  {:author "Matt Halverson"
   :date "Fri Aug 15 2014"})

(defrecord FileListing [paths common-prefixes truncated? marker next-marker])

(defn make-file-listing
  ([]
     (make-file-listing (list)))
  ([paths]
     (make-file-listing paths nil))
  ([paths common-prefixes]
     (make-file-listing paths common-prefixes false))
  ([paths common-prefixes truncated?]
     (make-file-listing paths common-prefixes truncated? nil))
  ([paths common-prefixes truncated? marker]
     (make-file-listing paths common-prefixes truncated? marker nil))
  ([paths common-prefixes truncated? marker next-marker]
     (->FileListing paths
                    common-prefixes
                    truncated?
                    marker
                    next-marker)))

(defprotocol FileSystem
  (list-files-matching-prefix [_ prefix options]
    "Lists all files matching the specified prefix.

    Options is a map, with the following keys planning to be recognized:
      :delimiter, default to '/'
      :recursive, default to false
      :limit,     default to nil
      :regex-filter, default to nil
      :marker,    default to nil

    IMPORTANT: Right now, none of the options are actually implemented!

    Example of the default behavior:
    Let prefix='/foo/b' and the filetree be as follows:
      /
      |-foo/
         |-bar/
         |  |-file1.txt
         |  |-file2.txt
         |-baz/
         |  |-file3.txt
         |-not_a_match/
         |  |-file4.txt
         |-also_not_a_match.csv
         |-bark.csv
     Then the matches are:
     [/foo/bar/
      /foo/baz/
      /foo/bark.csv]

    The keys have the following semantics:
    :delimiter
     -> In the above example, the match only went to /foo/bar/
        rather than /foo/bar/file1.txt because the match STOPS
        at the first delimiter encountered after the prefix. If you
        are using a windows-fs where the delimiter is \\, or if you
        are using a custom delimiter in your s3-keys, then you will
        want to override the default

    :recursive
     -> When true, the matching behavior changes to s3 style prefix matching.
        It will ignore delimiters and list *all* filepaths that match. In
        other words, it doesn't stop when it encounters 'common-prefixes'...
        it recurs down into them!
        In the above example, the matches would be:
        [/foo/bar/file1.txt
         /foo/bar/file2.txt
         /foo/baz/file3.txt
         /foo/bark.csv]

    :limit
     -> You may want to limit the number of results you get. If so, specify
        how many results you want by setting the limit. See also :marker.

    :regex-filter
     -> Filters the results before they're returned to you -- keeps only
        the results that match the specified regex.

    :marker
     -> If you specify a :limit, you may want to pick up where you left off
        in a future query. To do so, specify the :marker to be whatever the
        :next-marker was that you got back from your limited query."))
