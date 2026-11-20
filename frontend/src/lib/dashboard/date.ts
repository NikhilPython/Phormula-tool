// lib/dashboard/date.ts

export function getMonthCompletedPctTillYesterday(
  timestamp?: number | string | Date | null,
  timeZone?: string
) {
  if (!timestamp || !timeZone) return 0;

  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return 0;

  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const get = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((part) => part.type === type)?.value || "";

  const year = Number(get("year"));
  const month = Number(get("month"));
  const day = Number(get("day"));

  if (!year || !month || !day) return 0;

  const completedDay = Math.max(day - 1, 0);
  const daysInMonth = new Date(year, month, 0).getDate();

  return daysInMonth > 0 ? (completedDay / daysInMonth) * 100 : 0;
}

export function getISTYearMonth() {
  const optsMonth: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    month: "long",
  };
  const optsYear: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    year: "numeric",
  };
  const now = new Date();
  const monthName = now.toLocaleString("en-US", optsMonth);
  const yearStr = now.toLocaleString("en-US", optsYear);
  return { monthName, year: Number(yearStr) };
}

export function getPrevISTYearMonth() {
  const tz = "Asia/Kolkata";
  const now = new Date();
  const istNow = new Date(now.toLocaleString("en-US", { timeZone: tz }));
  const year =
    istNow.getMonth() === 0 ? istNow.getFullYear() - 1 : istNow.getFullYear();
  const monthIdx = istNow.getMonth() === 0 ? 11 : istNow.getMonth() - 1;
  const monthName = new Date(year, monthIdx, 1).toLocaleString("en-US", {
    month: "long",
    timeZone: tz,
  });
  return { monthName, year };
}

export function getPrevMonthShortLabel() {
  const { monthName, year } = getPrevISTYearMonth();
  const shortMon = new Date(`${monthName} 1, ${year}`).toLocaleString("en-US", {
    month: "short",
    timeZone: "Asia/Kolkata",
  });
  return `${shortMon}'${String(year).slice(-2)}`;
}

export function getThisMonthShortLabel() {
  const now = new Date();
  const istString = now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
  const istDate = new Date(istString);

  const monthName = istDate.toLocaleString("en-US", {
    month: "long",
    timeZone: "Asia/Kolkata",
  });

  const year = istDate.getFullYear();

  const shortMon = new Date(`${monthName} 1, ${year}`).toLocaleString(
    "en-US",
    {
      month: "short",
      timeZone: "Asia/Kolkata",
    }
  );

  return `${shortMon}'${String(year).slice(-2)}`;
}

export function getISTDayInfo() {
  const tz = "Asia/Kolkata";
  const now = new Date();
  const istNow = new Date(now.toLocaleString("en-US", { timeZone: tz }));
  const todayDay = istNow.getDate();
  const { monthName, year } = getPrevISTYearMonth();
  const prevMonthIdx = new Date(`${monthName} 1, ${year}`).getMonth();
  const daysInPrevMonth = new Date(year, prevMonthIdx + 1, 0).getDate();
  const daysInThisMonth = new Date(
    istNow.getFullYear(),
    istNow.getMonth() + 1,
    0
  ).getDate();
  return { todayDay, daysInPrevMonth, daysInThisMonth };
}
