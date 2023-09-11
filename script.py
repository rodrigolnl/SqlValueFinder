from ValueFinder import ValueFinder


finder = ValueFinder('BGB-HOM-DB01', number_of_threads=50)
findings = finder.find_value('EB1B85C352FD417FB2702C6936A73A1A', databases=['DBPortal'], tables=[], exact_match=False)
