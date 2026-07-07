import React from "react";

interface DeleteProps {
  className: string;
  onClick: () => void;
}

const Delete = ({className , onClick}: DeleteProps) => {
  return (
    <div
      className={`${className} flex items-center justify-center`}
       onClick={onClick}
    >
      <img
        src="/images/chatbot/Rectangle.png"
      className="absolute w-10 h-10 sm:w-10 sm:h-10 md:w-12 md:h-12 lg:h-14 lg:w-14"
        alt="Ellipse "
      />
      <img
        src="/images/chatbot/Delete.png"
         className="relative w-5 h-5 sm:w-5 sm:h-5 md:w-7 md:h-7 lg:w-8 lg:h-8"
        alt="Plane"
      />
    </div>
  );
};

export default Delete;
