export function buildCountryMarketplaceMap(
  countries?: string[] | string,
  marketplaces?: string[] | string
) {
  if (!countries || !marketplaces) return {};

  const countryArr = Array.isArray(countries)
    ? countries.map(c => c.toLowerCase())
    : countries.split(",").map(c => c.trim().toLowerCase());

  const marketplaceArr = Array.isArray(marketplaces)
    ? marketplaces
    : marketplaces.split(",").map(m => m.trim());

  const map: Record<string, string> = {};
  countryArr.forEach((c, i) => {
    if (marketplaceArr[i]) map[c] = marketplaceArr[i];
  });

  return map;
}
