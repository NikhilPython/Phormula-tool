import React from "react";
import Svg, { Path, Circle, Rect } from "react-native-svg";

type IconProps = {
  color?: string;
  size?: number;
};

export const WalletIcon = ({ color = "#000", size = 24 }: IconProps) => (
  <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
    <Rect x="3" y="6" width="18" height="12" rx="3" stroke={color} strokeWidth="2" />
    <Path d="M3 10H21" stroke={color} strokeWidth="2" strokeLinecap="round" />
    <Path d="M7 14H11" stroke={color} strokeWidth="2" strokeLinecap="round" />
  </Svg>
);

export const AnalyticsIcon = ({ color = "#000", size = 24 }: IconProps) => (
  <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
    <Path d="M4 16L10 10L14 14L20 8" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    <Path d="M16 8H20V12" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
  </Svg>
);

export const PieChartIcon = ({ color = "#000", size = 24 }: IconProps) => (
  <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
    <Path d="M21 12C21 16.9706 16.9706 21 12 21C7.02944 21 3 16.9706 3 12C3 7.02944 7.02944 3 12 3V12H21Z" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
  </Svg>
);

export const ScanIcon = ({ color = "#000", size = 24 }: IconProps) => (
  <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
    <Path d="M4 8V6C4 4.89543 4.89543 4 6 4H8" stroke={color} strokeWidth="2" strokeLinecap="round" />
    <Path d="M16 4H18C19.1046 4 20 4.89543 20 6V8" stroke={color} strokeWidth="2" strokeLinecap="round" />
    <Path d="M20 16V18C20 19.1046 19.1046 20 18 20H16" stroke={color} strokeWidth="2" strokeLinecap="round" />
    <Path d="M8 20H6C4.89543 20 4 19.1046 4 18V16" stroke={color} strokeWidth="2" strokeLinecap="round" />
    <Path d="M8 12H16" stroke={color} strokeWidth="2" strokeLinecap="round" />
  </Svg>
);


export const GridIcon = ({ color = "#000", size = 24 }: IconProps) => (
  <Svg width={size} height={size} viewBox="0 0 24 24" fill="none">
    <Path 
      d="M8.4 3H5.6A1.6 1.6 0 0 0 4 4.6v4.8A1.6 1.6 0 0 0 5.6 11h2.8A1.6 1.6 0 0 0 10 9.4V4.6A1.6 1.6 0 0 0 8.4 3Z" 
      fill="none" 
      stroke={color} 
      strokeWidth="1.5" 
      strokeMiterlimit="10" 
    />
    <Path 
      d="M8.4 15H5.6A1.6 1.6 0 0 0 4 16.6v2.8A1.6 1.6 0 0 0 5.6 21h2.8a1.6 1.6 0 0 0 1.6-1.6v-2.8A1.6 1.6 0 0 0 8.4 15ZM18.4 3h-2.8A1.6 1.6 0 0 0 14 4.6v2.8A1.6 1.6 0 0 0 15.6 9h2.8A1.6 1.6 0 0 0 20 7.4V4.6A1.6 1.6 0 0 0 18.4 3Z" 
      fill="none" 
      stroke={color} 
      strokeWidth="1.5" 
      strokeMiterlimit="10" 
    />
    <Path 
      d="M18.4 13h-2.8a1.6 1.6 0 0 0-1.6 1.6v4.8a1.6 1.6 0 0 0 1.6 1.6h2.8a1.6 1.6 0 0 0 1.6-1.6v-4.8a1.6 1.6 0 0 0-1.6-1.6Z" 
      fill="none" 
      stroke={color} 
      strokeWidth="1.5" 
      strokeMiterlimit="10" 
    />
  </Svg>
);