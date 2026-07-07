"use client";

import React from "react";
import { FiEdit, FiCheck, FiX } from "react-icons/fi";
import Button from "../ui/button/Button";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// yahan props ke through sab data pass kar do
export default function ObjectivesSection({
  monthlyTargetColumns,
  monthlyTargetData,
  ratesLoading,
  isTargetEditMode,
  openTargetEditMode,
  saveInlineTarget,
  closeTargetEditMode,
  isSaving,
  editingPid,
  isObjectiveEditMode,
  startObjectiveEdit,
  handleInlineObjectiveSave,
  cancelObjectiveEdit,
  objectiveDraft,
  setObjectiveDraft,
  objective,
  integratedCountries,
  prettifyObjectiveValue,
}: any) {
  return (
    <div className="grid grid-cols-1 gap-4">
      <div>
        <div className="rounded-2xl border border-gray-200 bg-white">
          <div className="flex items-center justify-between px-4 py-3">
            <PageBreadcrumb pageTitle="Monthly Targets" variant="table" align="left" />
            <div className="flex items-center gap-2">
              {!isTargetEditMode ? (
                <button
                  type="button"
                  onClick={openTargetEditMode}
                  className="inline-flex h-9 w-9 items-center justify-center text-gray-700"
                >
                  <FiEdit className="text-lg" />
                </button>
              ) : (
                <>
                  <Button
                    type="button"
                    onClick={saveInlineTarget}
                    size="icon"
                    disabled={isSaving || !editingPid}
                  >
                    <FiCheck />
                  </Button>

                  <Button
                    type="button"
                    onClick={closeTargetEditMode}
                    size="icon"
                    variant="outline"
                    disabled={isSaving}
                  >
                    <FiX />
                  </Button>
                </>
              )}
            </div>
          </div>

          <div className="h-px w-full bg-gray-200" />

          <div className="p-4">
            <DataTable
              columns={monthlyTargetColumns}
              data={monthlyTargetData}
              paginate={false}
              scrollY={false}
              stickyHeader={false}
              emptyMessage={ratesLoading ? "Loading currency rates..." : "No connected marketplaces."}
              className="rounded-xl"
              rowClassName={(row: any) => (row.__isTotal ? "bg-[#D9D9D933] font-semibold" : "")}
            />
          </div>
        </div>
      </div>

      <div>
        <div className="rounded-2xl border border-gray-200 bg-white">
          <div className="flex items-center justify-between px-4 py-3">
            <PageBreadcrumb pageTitle="Strategic Objectives" variant="table" align="left" />
            {!isObjectiveEditMode ? (
              <button
                onClick={startObjectiveEdit}
                className="h-9 w-9 text-gray-700"
                type="button"
              >
                <FiEdit className="text-lg" />
              </button>
            ) : (
              <div className="flex items-center gap-2">
                <Button size="icon" onClick={handleInlineObjectiveSave}>
                  <FiCheck />
                </Button>
                <Button size="icon" variant="outline" onClick={cancelObjectiveEdit}>
                  <FiX />
                </Button>
              </div>
            )}
          </div>

          <div className="h-px w-full bg-gray-200" />

          <div className="p-4">
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <div>
                <p className="mb-1 text-xs text-gray-500">Growth</p>
                {isObjectiveEditMode ? (
                  <select
                    value={objectiveDraft.growth_intent}
                    onChange={(e) =>
                      setObjectiveDraft((prev: any) => ({
                        ...prev,
                        growth_intent: e.target.value,
                      }))
                    }
                    className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm"
                  >
                    <option value="conservative">Conservative</option>
                    <option value="balanced">Balanced</option>
                    <option value="aggressive">Aggressive</option>
                  </select>
                ) : (
                  <div className="text-sm font-medium text-gray-800">
                    {prettifyObjectiveValue(objective.growth_intent)}
                  </div>
                )}
              </div>

              <div>
                <p className="mb-1 text-xs text-gray-500">Country</p>
                {isObjectiveEditMode ? (
                  <select
                    value={objectiveDraft.country}
                    onChange={(e) =>
                      setObjectiveDraft((prev: any) => ({
                        ...prev,
                        country: e.target.value,
                      }))
                    }
                    className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm"
                  >
                    <option value="" disabled>
                      Select Country
                    </option>
                    {integratedCountries.map((c: string) => (
                      <option key={c} value={c}>
                        {c.toUpperCase()}
                      </option>
                    ))}
                  </select>
                ) : (
                  <div className="text-sm font-medium text-gray-800">
                    {objective.country?.toUpperCase() || "-"}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}