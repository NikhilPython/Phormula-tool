// Productinfoinpopup.tsx
import React, { useEffect, useState } from 'react';
import { useRouter, useParams, usePathname } from 'next/navigation'; // Next.js uses next/navigation
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import Loader from '@/components/loader/Loader';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

interface ProductDataPoint {
  month: string;
  net_sales: number;
}

interface CountryData {
  [country: string]: ProductDataPoint[];
}

interface ApiResponse {
  success: boolean;
  data?: {
    [country: string]: CountryData;
  };
  message?: string;
}

interface ProductinfoinpopupProps {
  productname?: string;
  onClose?: () => void;
}

const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({ productname = "Menthol", onClose }) => {
  const params = useParams();
  const pathname = usePathname();
  const router = useRouter();
  const { countryName, month, quarter, year } = params as { countryName?: string; month?: string; quarter?: string; year?: string };
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const authToken = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [showSearchResults, setShowSearchResults] = useState<boolean>(false);
  const [searchLoading, setSearchLoading] = useState<boolean>(false);

  // State for controls
  const [timeRange, setTimeRange] = useState<'Yearly' | 'Quarterly'>('Yearly');
  const selectedYear = parseInt(year as string) || new Date().getFullYear(); 
  const [selectedQuarter, setSelectedQuarter] = useState<string>('1');
  const [selectedCountries, setSelectedCountries] = useState<Record<string, boolean>>({
    uk: true,
    global: true
  });

  const GBP_TO_USD_RATE = 1.27;

  // Generate years (e.g., last 5 years)
  const years = Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i);
  const quarters = [
    { value: '1', label: 'Q1' },
    { value: '2', label: 'Q2' },
    { value: '3', label: 'Q3' },
    { value: '4', label: 'Q4' }
  ];

  const handleCountryChange = (country: string) => {
    setSelectedCountries(prev => ({
      ...prev,
      [country]: !prev[country]
    }));
  };

  // Search products function
  const searchProducts = async (query: string) => {
    if (!query.trim()) {
      setSearchResults([]);
      setShowSearchResults(false);
      return;
    }

    setSearchLoading(true);
    try {
      const response = await fetch(`http://localhost:5000/Product_search?query=${encodeURIComponent(query)}`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${authToken}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      setSearchResults(data.products || []);
      setShowSearchResults(true);
    } catch (err: any) {
      console.error('Search error:', err);
      setSearchResults([]);
      setShowSearchResults(false);
    } finally {
      setSearchLoading(false);
    }
  };

  // Handle search input change with debounce
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      searchProducts(searchQuery);
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [searchQuery]);

  const getCurrencySymbol = (country?: string) => {
    switch ((country || '').toLowerCase()) {
      case "uk":
        return "£";
      case "india":
        return "₹";
      case "us":
        return "$";
      case "europe":
      case "eu":
        return "€";
      case "global":
        return "$";
      default:
        return "¤"; // generic currency symbol
    }
  };
  const currencySymbol = countryName ? getCurrencySymbol(countryName) : '¤';

  const fetchProductData = async () => {
    setLoading(true);
    setError('');

    try {
      // Get selected countries as array
      const countries = Object.keys(selectedCountries).filter(country => selectedCountries[country]);

      // Prepare request payload
      const requestPayload = {
        product_name: productname,
        time_range: timeRange,
        year: selectedYear,
        quarter: timeRange === 'Quarterly' ? selectedQuarter : null,
        countries: countries
      };

      console.log('Sending request:', requestPayload);

      // Make API call to backend
      const response = await fetch(`http://localhost:5000/ProductwisePerformance`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${authToken}`
        },
        body: JSON.stringify(requestPayload)
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
      }

      const responseData: ApiResponse = await response.json();
      console.log('Received response:', responseData);

      if (responseData.success) {
        setData(responseData);
        console.log('Received response (pretty):', JSON.stringify(responseData, null, 2));
        console.log('Success:', responseData.success);
        console.log('Data:', responseData.data);  // if such a field exists
        console.log('Message:', responseData.message); // or any other known field

      } else {
        throw new Error('API returned unsuccessful response');
      }
    } catch (err: any) {
      console.error('API Error:', err);
      setError(err.message || 'Failed to fetch data from server');
    } finally {
      setLoading(false);
    }
  };

  const getQuarterMonths = (quarter: string) => {
    const quarterMap: Record<string, string[]> = {
      '1': ['January', 'February', 'March'],
      '2': ['April', 'May', 'June'],
      '3': ['July', 'August', 'September'],
      '4': ['October', 'November', 'December']
    };
    return quarterMap[quarter] || [];
  };

  useEffect(() => {
    fetchProductData();
  }, [productname, year]);

const prepareChartData = () => {
  if (!data || !data.data) return [];

  const monthOrder = [
    'January','February','March','April','May','June',
    'July','August','September','October','November','December'
  ];

  const allMonths = new Set<string>();

  // 1) Collect months from ANY arrays under each country
  Object.values(data.data as any).forEach((countryBlock: any) => {
    if (!countryBlock) return;

    // Normalize to: list of arrays of rows
    const arrays: any[][] = [];

    if (Array.isArray(countryBlock)) {
      arrays.push(countryBlock);
    } else {
      // countryBlock is an object like { Yearly: [...], Quarterly: [...] }
      Object.values(countryBlock).forEach((maybeArr: any) => {
        if (Array.isArray(maybeArr)) arrays.push(maybeArr);
      });
    }

    arrays.forEach(rows => {
      rows.forEach((m: any) => {
        if (m?.month) allMonths.add(String(m.month));
      });
    });
  });

  const labels = Array.from(allMonths).sort(
    (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
  );

  if (!labels.length) return [];

  const getMetric = (country: string, month: string) => {
    const countryBlock: any = (data.data as any)[country];
    if (!countryBlock) return 0;

    let rows: any[] = [];

    if (Array.isArray(countryBlock)) {
      rows = countryBlock;
    } else {
      // pick first array inside the object
      const firstArr = Object.values(countryBlock).find((v: any) => Array.isArray(v)) as any[] | undefined;
      if (firstArr) rows = firstArr;
    }

    const found = rows.find((m: any) => m.month === month);
    return found ? Number(found.net_sales || 0) : 0;
  };

  return labels.map((month) => {
    const ukRaw = getMetric('uk', month);
    const usRaw = getMetric('us', month);
    const globalRaw = getMetric('global', month);

    const ukUSD = ukRaw * GBP_TO_USD_RATE;
    const globalUSD = (ukUSD + usRaw) !== 0 ? (ukUSD + usRaw) : globalRaw;

    const point: Record<string, any> = { month };

    if (selectedCountries.uk) point.uk = ukUSD;
    if (selectedCountries.us) point.us = usRaw;
    if (selectedCountries.global) point.global = globalUSD;

    return point;
  });
};



  const getCountryColor = (country: string) => {
    const colors: Record<string, string> = {
      uk: '#AB64B5',
      us: '#87AD12',
      global: '#F47A00'
    };
    return colors[country] || '#ff7c7c';
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const buildChartJSData = () => {
    const raw = prepareChartData();
    if (!raw || raw.length === 0) return null;

    const labels = raw.map(item => item.month);
  const datasets = Object.keys(selectedCountries)
  .filter(country => selectedCountries[country])
  .filter(country => Array.isArray((data?.data as any)?.[country]) && ((data?.data as any)?.[country].length > 0))
      .map(country => ({
        label: country.toUpperCase(),
        data: raw.map(item => item[country] || 0),
        borderColor: getCountryColor(country),
        backgroundColor: getCountryColor(country),
        tension: 0.1,
        pointRadius: 3,
        // pointHoverRadius: 5,
        fill: false,
         borderDash: country === "global" ? [6, 4] : undefined,
  borderWidth: country === "global" ? 2.5 : 2,
      } as const));

    return { labels, datasets };
  };

  const chartJSData = buildChartJSData();

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: false, // as per your config
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const value = context.parsed.y;
            return `${context.dataset.label}: ${formatCurrency(value)}`;
          }
        }
      }
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Month'
        }
      },
      y: {
        title: {
          display: true,
          text: `Amount (${currencySymbol})`
        },
        min: 0,
        ticks: {
          padding: 0
        }
      }
    }
  };

  const isImprovementsPage = pathname?.includes("mprovements") || false;

  return (
    <>
      <style>{`
.net-sales-wrapper {
  width: 100%;
  margin-bottom: 15px;
}

.net-sales-content {
  display: flex;
  justify-content: space-between; /* Left and right sections */
  align-items: flex-start;
  flex-wrap: wrap;
  gap: 16px;
}

.net-sales-left {
  flex: 1 1 auto;
}

.net-sales-header {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
}

.net-sales-title {
  margin: 0;
  font-size: 18px;
  font-family: 'Lato', sans-serif;
  color: #414042;
  background-color: white;
  border-radius: 7px;
  font-weight: bold;
  padding:0px;
}

.net-sales-subtitle {
  font-size: 0.875rem;
  color: #6b7280;
  margin: 0;
  font-style: italic;
}

.highlighted {
  color: #5ea68e;
  font-weight: 500;
}

.net-sales-right {
  display: flex;
  align-items: center;
  justify-content: flex-end;
}

.country-toggle-group {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  justify-content: flex-end;
}


.highlighted {
  color: #5ea68e;
  font-weight: bold;
}


.country-toggle-group {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
}

  .label{
    font-weight: bold;
    }

.country-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.9vw;
  color: #111827;
  cursor: pointer;
  font-weight: 600;
  padding: 5px 10px;
  border-radius: 16px;
  transition: all 0.2s ease-in-out;
  user-select: none;
  --country-color: #ccc; /* fallback */
}

.country-toggle input[type="checkbox"] {
  appearance: none;
  width: 13px;
  height: 13px;
  margin: 0;
  border: none;
  border-radius: 2px;
  background-color: var(--country-color);
  display: grid;
  place-content: center;
  cursor: pointer;
  transition: background-color 0.2s;
  position: relative;
}

.country-toggle input[type="checkbox"]::before {
  content: "✔";
font-size: 0.5vw;
  color: white;
  transform: scale(0);
  transition: transform 0.1s ease-in-out;
}

.country-toggle input[type="checkbox"]:checked {
  background-color: var(--country-color);
}

.country-toggle input[type="checkbox"]:checked::before {
  transform: scale(1);
  //  background-color: var(--country-color);
}

.country-label {
  color: var(--country-color);
  text-decoration: underline;
  text-decoration-thickness: 1.2px; /* Slightly thicker */
  text-underline-offset: 2px;
    font-size: 0.9vw;
    font-weight: 600;
}
.country-toggle .dot {
  width: 1vw;
  height: 1vw;
  border-radius: 50%;
  display: inline-block;
}

.country-toggle.active {
}

.country-toggle:hover {
}

/* Responsive */
@media (max-width: 640px) {
  .net-sales-content {
    flex-direction: column;
    align-items: flex-start;
  }

  .country-toggle-group {
    justify-content: flex-start;
  }
}




/* On small screens (phones), stack vertically */
@media (max-width: 600px) {
  .performance-header {
    flex-direction: column;
    align-items: flex-start;
  }

  .performance-title {
    width: 100%;
  }
}


.loading-spinner {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  width: 16px;
  height: 16px;
  border: 2px solid #e5e7eb;
  border-top: 2px solid #3b82f6;
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to {
    transform: translateY(-50%) rotate(360deg);
  }
}


.item-name {
  font-weight: 600;
  color: #1f2937;
}


/* Wrapper ensures full left alignment and spacing */
.filter-wrapper {
  display: flex;
  align-items: flex-end;
  gap: 16px;
  margin-bottom: 24px;
  flex-wrap: wrap;
 align-items: center;  /* Vertical alignment */ 
}

/* Filter container styled as a table-like block */
.filter-container {
  display: flex;
  background: white;
  border: 1px solid #414042;
  // border-radius: 6px;
  box-shadow: 0 2px 5px rgba(0,0,0,0.1);
  overflow: hidden;
  max-width: 640px;
  flex-wrap: wrap;
}

/* Dropdown section styles */
.dropdown-group {
text-align: center;
  flex: 1;
  min-width: 100px;
  border-right: 1px solid #414042;
}

.dropdown-group:last-child {
  border-right: none;
}


  option{
  text-align: center;
  }


.dropdown-select:hover {
  background-color: #f9fafb;
}


/* Mobile responsiveness */
@media (max-width: 600px) {
  .filter-wrapper {
    flex-direction: column;
    align-items: stretch;
  }

  .filter-container {
    flex-direction: column;
    width: 100%;
  }

  .dropdown-group {
    border-right: none;
    border-bottom: 1px solid #414042;
  }

  .dropdown-group:last-child {
    border-bottom: none;
  }

  .fetch-button {
    width: 100%;
  }
}

        .loading-spinner {
          display: inline-block;
          width: 12px;
          height: 12px;
          border: 2px solid rgba(255,255,255,0.3);
          border-top: 2px solid white;
          border-radius: 50%;
          animation: spin 1s linear infinite;
          margin-left: 8px;
        }

        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }

        @media (max-width: 768px) {
          .filter-container {
            flex-direction: column;
          }

          .dropdown-group {
            border-right: none;
            border-bottom: 1px solid #e0e0e0;
          }

          .dropdown-group:last-child {
            border-bottom: none;
          }

          .fetch-button {
            border-radius: 0 0 7px 7px;
            padding: 16px 24px;
          }
        }

h2 {
  font-size: 18px;
  font-family: 'Lato', sans-serif;
  color: #414042;
  background-color: white;
  border-radius: 7px;
  font-weight: bold;
}
    `}</style>

      <div className="modal-backdrop" onClick={onClose}>
        <div className="modal-content" onClick={(e) => e.stopPropagation()}>


          {/* Loading State */}
          {loading && (
           <div className="flex flex-col items-center justify-center py-12 text-center">
                        <Loader
                          src="/infinity-unscreen.gif"
                          size={150}
                          transparent
                          roundedClass="rounded-none"
                          backgroundClass="bg-transparent"
                          respectReducedMotion
                        />
                      </div>
          )}

          {/* Error State */}
          {error && (
            <div style={{
              backgroundColor: '#fef2f2',
              border: '1px solid #fecaca',
              borderRadius: '16px',
              padding: '24px',
              marginBottom: '32px'
            }}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ color: '#dc2626', fontSize: '1.25rem', marginRight: '12px' }}>❌</div>
                <p style={{ color: '#b91c1c', fontWeight: '500', margin: 0 }}>
                  {error}
                </p>
              </div>
            </div>
          )}

          {/* Results */}
          {data && !loading && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '32px' }}>


              {/* Chart */}
              <div>

                <div className="net-sales-wrapper">
                  <div className="net-sales-content">
                    <div className="net-sales-left">
                      <div className="net-sales-header">
                        <h3 className="net-sales-title">Net Sales Trend -
                          <b className="highlighted">  {productname} {" "}(
                    {timeRange === "Yearly"
                      ? `YTD ${selectedYear}`
                      : `Q${selectedQuarter}'${selectedYear}`})
                  </b></h3>                       
                      </div>
                    </div>

                    <div className="net-sales-right">
                      <div className="country-toggle-group">
                        {Object.entries(selectedCountries).map(([country, isSelected]) => {
                          const color = getCountryColor(country);
                          return (
                            <label
                              key={country}
                              className={`country-toggle ${isSelected ? "active" : ""}`}
                              style={{ ['--country-color' as string]: color }}
                            >
                              <input
                                type="checkbox"
                                checked={isSelected}
                                onChange={() => handleCountryChange(country)}
                              />
                              <span className="country-label">{country.toUpperCase()}</span>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                </div>
                <div style={{
                  height: 'auto', maxHeight: '500px', display: 'flex',
                  justifyContent: 'center', alignItems: 'center'
                }}>

                  {chartJSData ? (
                    <Line data={chartJSData} options={chartOptions} />
                  ) : (
                    <p>No chart data available</p>
                  )}

                </div>
                {!isImprovementsPage && (
                  <button className="styled-button"
                  onClick={() =>router.push(`/productwiseperformance/${productname}/${countryName}/${month}/${year}`)}>
                  Check Full Performance{" "}
                  <i className="fa-solid fa-arrow-up-right-from-square"></i>
                </button>
                )}
              </div>


            </div>
          )}
        </div>
      </div>


    </>
  );
};

export default Productinfoinpopup;