'use client';

import React, { useRef, useState } from 'react';
import * as XLSX from 'xlsx';
import axios from 'axios';

const MONTHS = [
  'January','February','March','April','May','June',
  'July','August','September','October','November','December',
] as const;
const YEARS = Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i);

export default function UploadLocalInvModal({
  countryName,
  onClose,
  onSuccessNavigate,
}: {
  countryName: string;
  onClose: () => void;
  onSuccessNavigate: (month: string, year: string) => void;
}) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [errorStatus, setErrorStatus] = useState<number | null>(null);
  const [fileName, setFileName] = useState<string>('');
  const [selectedMonth, setSelectedMonth] = useState('');
  const [selectedYear, setSelectedYear] = useState('');

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    setFileName(file ? file.name : '');
  };

  const downloadTemplate = () => {
    const ws = XLSX.utils.aoa_to_sheet([
      ['Product Name', 'SKU_UK', 'SKU_US', 'Local Stock', 'In Transit Units'],
      ['', '', '', '', ''],
    ]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Stock Template');
    XLSX.writeFile(wb, 'Warehouse_Stock_Template.xlsx');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    const file = fileInputRef.current?.files?.[0];
    if (!file) {
      setMessage('Please select a file before submitting.');
      return;
    }
    if (!selectedMonth || !selectedYear) {
      setMessage('Please select both month and year.');
      return;
    }
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setMessage('Authorization token is missing. Please log in.');
      return;
    }

    try {
      setLoading(true);
      const formData = new FormData();
      formData.append('warehouse_balance', file);
      formData.append('month', selectedMonth);
      formData.append('year', selectedYear);
      formData.append('country', countryName);

      const response = await axios.post(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/purchase_order`,
        formData,
        {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'multipart/form-data',
          },
        },
      );

      if (response.data?.data) {
        setMessage('Purchase order generated successfully.');
        setErrorStatus(null);
        setTimeout(() => onSuccessNavigate(selectedMonth, selectedYear), 1200);
      }
    } catch (error: any) {
      if (error.response) {
        if (error.response.status === 404) {
          setMessage(
            'Please load the inventory forecast before generating the purchase order.',
          );
          setErrorStatus(404);
        } else {
          setMessage('Error: ' + (error.response?.data?.error || 'Unknown error'));
          setErrorStatus(error.response.status);
        }
      } else {
        setMessage('Unknown error');
        setErrorStatus(null);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="modal-overlay " onClick={onClose}>
      <div className="modal-content z-[100000]" onClick={(e) => e.stopPropagation()}>
        <div className="form-container">
          <div className="header-title">Upload Local Warehouse Stock Balance File</div>
          <form onSubmit={handleSubmit}>
            <div className="file-upload">
              <div className="upload-image-container">
                <p className="choose-text">{fileName || 'Choose File'}</p>
                <img src="/uploadbox.png" alt="Upload" className="upload-icon-image" />
              </div>
              <input
                ref={fileInputRef}
                type="file"
                accept=".xls,.xlsx,.csv"
                required
                onChange={handleFileChange}
              />
            </div>

            <div className="upload-hint-row">
              <em className="upload-hint-text">
                Please fill in the file with the correct data (SKU, Local Stock, In Transit Units) and upload it.
              </em>
              <button type="button" onClick={downloadTemplate} className="template-btn">
                Download (.xlsx)
              </button>
            </div>

            <div className="form-group">
              <label>Country</label>
              <input type="text" value={countryName.toUpperCase()} readOnly />
            </div>

            <div className="form-group-row">
              <div className="form-group">
                <label>Select Month</label>
                <select
                  value={selectedMonth}
                  onChange={(e) => setSelectedMonth(e.target.value)}
                  required
                >
                  <option value="">Select Month</option>
                  {MONTHS.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label>Select Year</label>
                <select
                  value={selectedYear}
                  onChange={(e) => setSelectedYear(e.target.value)}
                  required
                >
                  <option value="">Select Year</option>
                  {YEARS.map((y) => (
                    <option key={y} value={y}>
                      {y}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {message && (
              <div className={`message ${errorStatus ? 'error' : 'success'}`}>
                {message}
              </div>
            )}

            <button type="submit" disabled={loading} className="submit-btn">
              {loading ? 'Uploading...' : 'Upload File'}
            </button>
          </form>
        </div>
      </div>

      <style jsx>{`
        .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.5); display:flex; align-items:center; justify-content:center; z-index:10000; }
        .modal-content { background:#fff; max-width:700px; width:92%; max-height:90vh; overflow:auto; border-radius:10px; box-shadow:0 10px 30px rgba(0,0,0,.2); }
        .form-container{ margin:auto; padding:30px; box-sizing:border-box; border:2px solid #5EA68E; border-radius:15px; background:#fff; }
        .header-title{ text-align:center; font-size:24px; color:#5EA68E; font-weight:600; margin-bottom:10px; }
        .file-upload{ width:100%; padding:5px; border:1.5px dashed #bbb; border-radius:12px; position:relative; margin-bottom:10px; }
        .upload-image-container{ background:#fff; position:relative; display:flex; flex-direction:column; align-items:center; justify-content:center; }
        .upload-icon-image{ width:150px; height:150px; object-fit:contain; pointer-events:none; }
        .choose-text{ position:absolute; top:50px; font-size:14px; font-weight:500; color:#333; }
        .upload-hint-row{ display:flex; justify-content:space-between; align-items:center; margin:10px 0 20px; }
        .upload-hint-text{ font-size:12px; color:#444; font-style:italic; max-width:80%; }
        .template-btn{ background:#21304b; color:#f8edcf; border:none; padding:6px 12px; font-size:13px; border-radius:6px; cursor:pointer; }
        .form-group{ margin-bottom:15px; }
        .form-group label{ display:block; margin-bottom:8px; font-weight:500; }
        .form-group select, .form-group input{ width:100%; padding:10px 12px; font-size:16px; border:1px solid #999; border-radius:10px; }
        .form-group-row{ display:flex; gap:15px; }
        .form-group-row .form-group{ flex:1; }
        .submit-btn{ width:100%; background:#21304b; color:#f8edcf; padding:12px; font-size:16px; border:none; border-radius:10px; font-weight:600; margin-top:10px; cursor:pointer; }
        .message{ margin:15px 0; padding:10px; border-radius:5px; text-align:center; font-weight:500; }
        .error{ color:red; } .success{ color:green; }
        @media(max-width: 768px){ .form-container{ padding:20px } .form-group-row{ flex-direction:column } }
      `}</style>
    </div>
  );
}
