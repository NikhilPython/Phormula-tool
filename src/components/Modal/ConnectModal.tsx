// components/ConnectModal.tsx
'use client';

import React, { useState } from 'react';
import AmazonConnect from '../../features/integration/AmazonConnect'; // Adjust path as needed

interface ConnectModalProps {
  isOpen: boolean;
  onClose: () => void;
   ariaLabelledBy?: string;
   id?: string;
}

const ConnectModal: React.FC<ConnectModalProps> = ({ isOpen, onClose }) => {
  const [selectedPlatform, setSelectedPlatform] = useState<string | null>(null);

  const modalStyles: { [key: string]: React.CSSProperties } = {
    overlay: {
      position: 'fixed' as const,
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.7)',
      backdropFilter: 'blur(5px)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1000,
    },
    content: {
      backgroundColor: 'white',
      padding: '2rem',
      borderRadius: '12px',
      width: '90vw',
      maxWidth: '500px',
      maxHeight: '80vh',
      overflowY: 'auto',
      position: 'relative' as const,
      boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
    },
    closeButton: {
      position: 'absolute' as const,
      top: '15px',
      right: '20px',
      background: 'none',
      border: 'none',
      fontSize: '1.5rem',
      cursor: 'pointer',
      color: '#666',
      zIndex: 1001,
    },
    title: {
      fontSize: '1.5rem',
      fontWeight: 'bold',
      marginBottom: '1.5rem',
      textAlign: 'center',
      color: '#414042',
    },
    platformGrid: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gap: '1rem',
      marginBottom: '1rem',
    },
    platformCard: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      padding: '1.5rem',
      border: '2px solid #e0e0e0',
      borderRadius: '8px',
      cursor: 'pointer',
      transition: 'all 0.3s ease',
      backgroundColor: '#f9f9f9',
    },
    platformCardHover: {
      borderColor: '#0b5563',
      backgroundColor: '#f0f8ff',
    },
    platformIcon: {
      width: '60px',
      height: '60px',
      marginBottom: '1rem',
      objectFit: 'contain',
    },
    platformName: {
      fontSize: '1.1rem',
      fontWeight: 'bold',
      color: '#414042',
      marginBottom: '0.5rem',
    },
    platformDescription: {
      fontSize: '0.9rem',
      color: '#666',
      textAlign: 'center',
    },
    backButton: {
      position: 'absolute' as const,
      top: '15px',
      left: '20px',
      background: 'none',
      border: 'none',
      fontSize: '1.2rem',
      cursor: 'pointer',
      color: '#666',
      display: 'flex',
      alignItems: 'center',
      gap: '0.5rem',
    },
  };

  const handlePlatformSelect = (platform: string) => {
    setSelectedPlatform(platform);
  };

  const handleBack = () => {
    setSelectedPlatform(null);
  };

  const handleClose = () => {
    setSelectedPlatform(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div style={modalStyles.overlay} onClick={handleClose}>
      <div style={modalStyles.content} onClick={(e: React.MouseEvent) => e.stopPropagation()}>
        {/* <button style={modalStyles.closeButton} onClick={handleClose}>
          <i className="fa-solid fa-xmark"></i>
        </button> */}

        {selectedPlatform && (
          <button style={modalStyles.backButton} onClick={handleBack}>
            <i className="fa-solid fa-arrow-left"></i>
            Back
          </button>
        )}

        {!selectedPlatform ? (
          <>
            <h2 style={modalStyles.title}>Connect Your Store</h2>
            <div style={modalStyles.platformGrid}>
              <div
                style={modalStyles.platformCard}
                onClick={() => handlePlatformSelect('amazon')}
                onMouseEnter={(e: React.MouseEvent<HTMLDivElement>) => {
                  (e.target as HTMLElement).style.borderColor = '#0b5563';
                  (e.target as HTMLElement).style.backgroundColor = '#f0f8ff';
                }}
                onMouseLeave={(e: React.MouseEvent<HTMLDivElement>) => {
                  (e.target as HTMLElement).style.borderColor = '#e0e0e0';
                  (e.target as HTMLElement).style.backgroundColor = '#f9f9f9';
                }}
              >
                <div style={modalStyles.platformIcon}>
                  <i className="fa-brands fa-amazon" style={{ fontSize: '3rem', color: '#FF9900' }}></i>
                </div>
                <div style={modalStyles.platformName}>Amazon</div>
                <div style={modalStyles.platformDescription}>
                  Connect your Amazon Seller Central account
                </div>
              </div>

              <div
                style={modalStyles.platformCard}
                onClick={() => handlePlatformSelect('shopify')}
                onMouseEnter={(e: React.MouseEvent<HTMLDivElement>) => {
                  (e.target as HTMLElement).style.borderColor = '#0b5563';
                  (e.target as HTMLElement).style.backgroundColor = '#f0f8ff';
                }}
                onMouseLeave={(e: React.MouseEvent<HTMLDivElement>) => {
                  (e.target as HTMLElement).style.borderColor = '#e0e0e0';
                  (e.target as HTMLElement).style.backgroundColor = '#f9f9f9';
                }}
              >
                <div style={modalStyles.platformIcon}>
                  <i className="fa-brands fa-shopify" style={{ fontSize: '3rem', color: '#7AB55C' }}></i>
                </div>
                <div style={modalStyles.platformName}>Shopify</div>
                <div style={modalStyles.platformDescription}>
                  Connect your Shopify store
                </div>
              </div>
            </div>
          </>
        ) : selectedPlatform === 'amazon' ? (
          <AmazonConnect onClose={handleClose} />
        ) : (
          ""
        )}
      </div>
    </div>
  );
};

export default ConnectModal;