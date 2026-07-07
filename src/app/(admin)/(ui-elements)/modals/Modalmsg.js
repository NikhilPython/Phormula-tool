import { useEffect } from 'react';

const Modalmsg = ({ show, message, onClose, onCancel }) => {
  const messageText = typeof message === 'string' ? message.toLowerCase() : '';

  // Auto-close modal after 1 second for specific success messages
  useEffect(() => {
    if (show && (messageText.includes("file uploaded successfully") || messageText.includes("saved and applied"))) {
      const timer = setTimeout(() => {
        onClose();
      }, 1000);

      return () => clearTimeout(timer);
    }
  }, [show, messageText, onClose]);

  if (!show) return null;

  return (
    <>
      <style>{`
        .modal-backdrop {
          position: fixed;
          top: 0;
          left: 0;
          width: 100%;
          height: 100%;
          background: rgba(0, 0, 0, 0.4);
          display: flex;
          justify-content: center;
          align-items: center;
          z-index: 999;
        }

        .modal-box {
          background: #fff;
          padding: 20px 30px;
          border-radius: 8px;
          text-align: center;
          max-width: 400px;
          box-shadow: 0 0 10px rgba(0,0,0,0.3);
        }
        .modal-box p {
          color: #414042;
          font-size: 14px;
        }

        .close-btn {
          padding: 10px 20px;
          background-color: #2c3e50;
          color: #fdf6e4;
          font-size: 16px;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.2);
          cursor: pointer;
          text-align: center;
          font-size: 12px;
          font-weight: bold;
        }

        .close-btn:hover {
          background-color: #2e455bff;
        }

        .button-groupofmodal {
          display: flex;
        }

        .close-btn {
          flex: 1;
        }

        .single-btn {
          max-width: none;
          flex: none;
          margin: 0 auto;
        }
      `}</style>

      <div className="modal-backdrop">
        <div className="modal-box">
          <p>{message}</p>

           <div className="button-groupofmodal">
            {!(
              messageText.includes("file uploaded successfully") ||
              messageText.includes("saved and applied")
            ) && (
              <>
                <button
                  onClick={onClose}
                  className={`close-btn ${messageText.includes("file uploaded successfully") ||
                    messageText.includes("saved and applied") ? "single-btn" : ""
                    }`}
                >
                  OK
                </button>
                &nbsp;
                <button onClick={onCancel} className="close-btn">Cancel</button>
              </>
            )}
          </div>

        </div>
      </div>
    </>
  );
};

export default Modalmsg;
