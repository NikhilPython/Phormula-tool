'use client';
import { useParams } from 'next/navigation';
import { useMemo } from 'react';
import DisplayInventoryForecast from './DisplayInventoryForecast';

export default function InventoryManualResult({
  inlineData,
  inlineCountry,
  inlineMonth,
  inlineYear,
}: {
  inlineData: any[];
  inlineCountry: string;
  inlineMonth: string;
  inlineYear: string;
}) {
  const params = useParams() as { countryName?: string; month?: string; year?: string };

  const rows = useMemo(() => {
    if (Array.isArray(inlineData) && inlineData.length > 0) return inlineData;
    try {
      const s = (typeof window !== 'undefined' && sessionStorage.getItem('manualForecastRows')) || '[]';
      const parsed = JSON.parse(s);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }, [inlineData]);

  const countryName = inlineCountry ?? params.countryName ?? '';
  const month = inlineMonth ?? params.month ?? '';
  const year = inlineYear ?? params.year ?? '';

  if (!rows || rows.length === 0) {
    return <div className="p-6 text-sm text-gray-600">No data available. Please submit your manual forecast.</div>;
  }

  return (
    <DisplayInventoryForecast
      countryName={countryName}
      month={month}
      year={year}
      data={rows}
    />
  );
}
