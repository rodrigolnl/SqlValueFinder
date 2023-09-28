from ValueFinder import ValueFinder


finder = ValueFinder('BGB-HOM-DB01', number_of_threads=5)
findings = finder.find_value('Elize', databases=['DBOPEN'], tables=[], exact_match=False)
