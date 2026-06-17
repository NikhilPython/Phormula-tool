import React, { ReactNode } from "react";

interface TableProps extends React.TableHTMLAttributes<HTMLTableElement> {
  children: ReactNode;
}

interface TableHeaderProps extends React.HTMLAttributes<HTMLTableSectionElement> {
  children: ReactNode;
}

interface TableBodyProps extends React.HTMLAttributes<HTMLTableSectionElement> {
  children: ReactNode;
}

interface TableRowProps extends React.HTMLAttributes<HTMLTableRowElement> {
  children: ReactNode;
}

interface TableCellProps
  extends React.ThHTMLAttributes<HTMLTableCellElement>,
  React.TdHTMLAttributes<HTMLTableCellElement> {
  children: ReactNode;
  isHeader?: boolean;
}

const Table: React.FC<TableProps> = ({ children, className = "", ...props }) => {
  return (
    <table className={`min-w-full ${className}`} {...props}>
      {children}
    </table>
  );
};

const TableHeader: React.FC<TableHeaderProps> = ({
  children,
  className = "",
  ...props
}) => {
  return (
    <thead className={className} {...props}>
      {children}
    </thead>
  );
};

const TableBody: React.FC<TableBodyProps> = ({
  children,
  className = "",
  ...props
}) => {
  return (
    <tbody className={className} {...props}>
      {children}
    </tbody>
  );
};

const TableRow: React.FC<TableRowProps> = ({
  children,
  className = "",
  ...props
}) => {
  return (
    <tr className={className} {...props}>
      {children}
    </tr>
  );
};

const TableCell: React.FC<TableCellProps> = ({
  children,
  isHeader = false,
  className = "",
  ...props
}) => {
  if (isHeader) {
    return (
      <th className={className} {...props}>
        {children}
      </th>
    );
  }

  return (
    <td className={className} {...props}>
      {children}
    </td>
  );
};

export { Table, TableHeader, TableBody, TableRow, TableCell };