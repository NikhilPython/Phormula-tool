// Convert ISO 2-letter country code (e.g. "IN") to emoji flag 🇮🇳
export function countryCodeToFlagEmoji(iso2: string): string {
  if (!iso2) return "";
  return iso2
    .toUpperCase()
    .replace(/./g, (char) =>
      String.fromCodePoint(127397 + char.charCodeAt(0))
    );
}
