from ValueFinder import ValueFinder


finder = ValueFinder('BGB-HOM-DB01', number_of_threads=50)
findings = finder.find_value('Rodrigo', databases=['Cadastro'], tables=None, exact_match=False)
