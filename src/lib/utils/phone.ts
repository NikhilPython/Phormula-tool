// Very lightweight approach: keep your previous behavior
export function formatPhoneNumber(raw: string) {
  if (!raw) return "";
  const onlyDigits = raw.replace(/[^0-9]/g, "");
  const cc = "+" + (onlyDigits.match(/^\d{1,3}/)?.[0] ?? "");
  const national = onlyDigits.slice(cc.replace("+", "").length);
  return `${cc} ${national}`.trim();
}
