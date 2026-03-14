'use client';

import React from 'react';
import SegmentedToggle from '@/components/ui/SegmentedToggle';

export type InventoryFlowTab = 'inventory' | 'dispatch' | 'purchaseOrder';

type InventoryFlowTabsProps = {
  value: InventoryFlowTab;
  onChange: (val: InventoryFlowTab) => void;
};

const TAB_OPTIONS: { value: InventoryFlowTab; label: string }[] = [
  { value: 'inventory', label: 'Inventory Forecast' },
  { value: 'dispatch', label: 'Dispatch' },
  { value: 'purchaseOrder', label: 'Purchase Order' },
];

export default function InventoryFlowTabs({
  value,
  onChange,
}: InventoryFlowTabsProps) {
  return (
    <SegmentedToggle<InventoryFlowTab>
      value={value}
      options={TAB_OPTIONS}
      onChange={onChange}
      className="w-full"
      textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
      compact
    />
  );
}