'use client';

import React from 'react';
import SegmentedToggle from '@/components/ui/SegmentedToggle';

export type InventoryFlowTab = 'inventory' | 'dispatch' | 'purchaseOrder';

type InventoryFlowTabsProps = {
  value: InventoryFlowTab;
  onChange: (val: InventoryFlowTab) => void;
  className?: string;
};

const TAB_OPTIONS: { value: InventoryFlowTab; label: string }[] = [
  { value: 'inventory', label: 'Inventory Forecast' },
  { value: 'dispatch', label: 'Dispatch' },
  { value: 'purchaseOrder', label: 'Purchase Order' },
];

export default function InventoryFlowTabs({
  value,
  onChange,
  className = 'w-full',
}: InventoryFlowTabsProps) {
  return (
    <SegmentedToggle<InventoryFlowTab>
      value={value}
      options={TAB_OPTIONS}
      onChange={onChange}
      className={className}
      textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
      compact
    />
  );
}
