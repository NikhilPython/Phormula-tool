export interface GrowthCategory {
  category: string;
  value: number;
}

export interface SkuItem {
  product_name: string;
  sku?: string;
  [key: string]: any;
}

export interface CategorizedGrowth {
  top_80_skus: SkuItem[];
  new_or_reviving_skus: SkuItem[];
  other_skus: SkuItem[];
  top_80_total?: SkuItem | null;
  new_or_reviving_total?: SkuItem | null;
  other_total?: SkuItem | null;
  all_skus_total?: SkuItem | null;
}

export interface DailyPoint {
  date: string;
  quantity?: number;
  net_sales?: number;
}

export interface DailySeries {
  previous: DailyPoint[];
  current_mtd: DailyPoint[];
}

export interface PeriodInfo {
  label: string;
  start_date: string;
  end_date: string;
}

export interface SkuInsight {
  product_name: string;
  insight: string;
  [key: string]: any;
}

export interface ApiResponse {
  message?: string;
  periods?: {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  };
  categorized_growth?: CategorizedGrowth;
  ai_insights?: Record<string, SkuInsight>;
  daily_series?: DailySeries;
  overall_summary?: string[];
  overall_actions?: string[];
}
