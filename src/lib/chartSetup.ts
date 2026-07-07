// src/lib/chartSetup.ts
"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  ChartTitle,
  Tooltip,
  Legend
);

// ✅ Global tooltip styles
ChartJS.defaults.plugins.tooltip.backgroundColor = "#ffffff";
ChartJS.defaults.plugins.tooltip.titleColor = "#414042";
ChartJS.defaults.plugins.tooltip.bodyColor = "#414042";
ChartJS.defaults.plugins.tooltip.borderColor = "#e5e7eb";
ChartJS.defaults.plugins.tooltip.borderWidth = 1;

export {}; 
