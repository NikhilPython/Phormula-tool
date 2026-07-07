// lib/services/inventoryApi.ts
'use client';
import { baseApi } from './baseApi';
import * as XLSX from 'xlsx';

/* ---------- Types ---------- */
export type CountryParams = { country: string; month: string; year: string };

export type UploadItem = {
  filename?: string;
  month?: string | number;
  year?: string | number;
  country?: string;
  [k: string]: any;
};
export type UploadHistoryRes = { uploads: UploadItem[] };

export type ForecastRow = Record<string, any>;

// JSON view coming from /api/forecast_allmonths -> {columns, data}
export type ForecastViewRes = { columns: string[]; data: ForecastRow[] };

export type ForecastRes = { forecast: ForecastRow[] } | ForecastRow[];

export type SaveHumanCagrReq = {
  updates: Array<{ sku: string; human_cagr: number; country: string }>;
};
export type ApplySelectedCagrReq = any[];

export type MonthRangeRes = { month_range?: string; [k: string]: any };

export type ManualPreviewReq = { country: string; month: string; year: string };
export type ManualSubmitReq = ManualPreviewReq & {
  growth: Record<string, number>;
};
export type ManualForecastRes = {
  rows?: any[];
  table?: any[];
  forecast?: any[];
  [k: string]: any;
};

/* helper: tolerant response parser for servers that send 200 with text/empty body */
const asJsonOrText = async (response: Response) => {
  const text = await response.text();
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    return { raw: text ?? null };
  }
};

const FORECAST_TAG = 'Forecast' as const;

/* -------------------- API -------------------- */
export const inventoryApi = baseApi.injectEndpoints({
  endpoints: (build) => ({

    /** 1) TRIGGER generation of the Excel (no JSON expected) */
    generateForecast: build.mutation<void, CountryParams>({
      query: ({ country, month, year }) => {
        const isGlobal = country.trim().toLowerCase() === 'global';
        const path = isGlobal
          ? `/forecast_global?month=${encodeURIComponent(month)}&year=${encodeURIComponent(year)}`
          : `/api/forecast?country=${encodeURIComponent(country)}&month=${encodeURIComponent(month)}&year=${encodeURIComponent(year)}`;
        return { url: path, method: 'GET' };
      },
      // invalidate the Forecast tag for this key so the view refetches
      invalidatesTags: (_res, _err, { country, month, year }) => [
        { type: FORECAST_TAG, id: `${country}-${month}-${year}` },
      ],
    }),

    /** 2) FETCH JSON VIEW for charts/tables -> /api/forecast_allmonths returns {columns, data} */
    getForecastView: build.query<ForecastViewRes, CountryParams>({
      query: ({ country, month, year }) =>
        `/api/forecast_allmonths?country=${encodeURIComponent(country)}&month=${encodeURIComponent(month)}&year=${encodeURIComponent(year)}`,
      transformResponse: (resp: any): ForecastViewRes => {
        const columns = Array.isArray(resp?.columns) ? resp.columns : [];
        const data = Array.isArray(resp?.data) ? resp.data : [];
        return { columns, data };
      },
      providesTags: (_res, _err, { country, month, year }) => [
        { type: FORECAST_TAG, id: `${country}-${month}-${year}` },
      ],
    }),

    /** 3) Save human CAGR values */
    saveHumanCagr: build.mutation<any, SaveHumanCagrReq>({
      query: (body) => ({ url: '/api/save_human_cagr', method: 'POST', body }),
      invalidatesTags: [FORECAST_TAG],
    }),

    /** 4) Apply selected CAGR set */
    applySelectedCagr: build.mutation<any, ApplySelectedCagrReq>({
      query: (payload) => ({ url: '/applied_cgr', method: 'POST', body: payload }),
      invalidatesTags: [FORECAST_TAG],
    }),

    /** 5) Month range helper (reads current-month Excel on server) */
    getMonthRange: build.query<MonthRangeRes, { country: string }>({
      query: ({ country }) => ({
        url: `/api/forecast_monthrange?country=${encodeURIComponent(country)}`,
        method: 'GET',
      }),
    }),

    /** 6) Manual preview (always JSON or tolerant text) */
    manualPreview: build.mutation<ManualForecastRes, ManualPreviewReq>({
      query: (body) => ({
        url: '/api/manual_forecast?preview=1',
        method: 'POST',
        body: { ...body, growth: {} },
        responseHandler: asJsonOrText,
        validateStatus: (res) => res.status >= 200 && res.status < 300,
      }),
      invalidatesTags: ['ManualForecast', FORECAST_TAG],
    }),

    /** 7) Manual submit – JSON OR Excel */
    manualSubmit: build.mutation<ManualForecastRes, ManualSubmitReq>({
      query: (body) => ({
        url: '/api/manual_forecast',
        method: 'POST',
        body,
        responseHandler: async (response: Response) => {
          const ctype = response.headers.get('content-type') ?? '';

          if (ctype.includes('application/json')) {
            const text = await response.text();
            try { return text ? JSON.parse(text) : {}; }
            catch { return { raw: text }; }
          }

          if (
            ctype.includes('application/vnd.openxmlformats') ||
            ctype.includes('octet-stream')
          ) {
            const blob = await response.blob();
            const buffer = await blob.arrayBuffer();
            const workbook = XLSX.read(buffer, { type: 'array' });
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonRows = XLSX.utils.sheet_to_json(sheet);
            return { rows: jsonRows };
          }

          const text = await response.text();
          return { raw: text };
        },
        validateStatus: (res) => res.status >= 200 && res.status < 300,
      }),
      invalidatesTags: ['ManualForecast', FORECAST_TAG],
    }),
  }),
  overrideExisting: true,
});

/* Hooks */
export const {
  useGenerateForecastMutation,
  useGetForecastViewQuery,
  useSaveHumanCagrMutation,
  useApplySelectedCagrMutation,
  useGetMonthRangeQuery,
  useManualPreviewMutation,
  useManualSubmitMutation,
} = inventoryApi;
